"""
Stack CDK: QuickSight BI Dashboard — Visualização de Dados

PRÉ-REQUISITO: Amazon QuickSight deve estar ativo na conta (Standard ou Enterprise).
  Acesse: https://quicksight.aws.amazon.com → "Sign up for QuickSight"
  Durante a inscrição, autorize acesso ao S3 e Athena.

  Passe o nome do usuário QuickSight via contexto:
    cdk deploy CloudMartQuickSight --context qs_user=<seu-usuario>
  (default: "Admin" — funciona se o primeiro usuário QuickSight foi criado com esse nome)

Recursos provisionados:
  1. Glue Database cloudmart_gold + Crawler para zona gold
       O crawler registra a tabela relatorio_vendas no Glue Catalog após o
       job silver_to_gold ser executado.
  2. IAM Role QuickSightServiceRole
       Acesso a Athena (workgroup cloudmart-analysts), S3 (gold + results)
       e Glue Catalog (cloudmart_gold).
  3. QuickSight DataSource → Athena (workgroup cloudmart-analysts)
  4. QuickSight DataSet → relatorio_vendas (DIRECT_QUERY via cloudmart_gold)
  5. QuickSight Analysis "Visão Executiva de Vendas"
       Sheet: Visão Executiva de Vendas
         • KPI      : Receita Total (R$)
         • KPI      : Total de Pedidos
         • KPI      : Ticket Médio (R$)
         • Barras H : Receita por Categoria
         • Barras H : Receita por Região
         • Linhas   : Tendência Mensal de Receita (por Categoria)
"""

from aws_cdk import (
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_quicksight as quicksight,
)
from constructs import Construct

# Identificador do dataset nos visuais (deve ser único dentro da Analysis)
_DS = "relatorio_vendas"


class QuickSightStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        account_id     = self.node.try_get_context("account_id") or self.account
        gold_bucket    = f"gold-zone-{account_id}"
        results_bucket = f"athena-results-cloudmart-{account_id}"

        # Nome de usuário QuickSight — obtido via contexto CDK
        qs_user     = self.node.try_get_context("qs_user") or "Admin"
        qs_user_arn = (
            f"arn:aws:quicksight:{self.region}:{account_id}:user/default/{qs_user}"
        )

        # ── 1. Glue: banco cloudmart_gold + crawler ───────────────────────────
        self._create_gold_catalog(account_id, gold_bucket)

        # ── 2. IAM Role para o QuickSight ────────────────────────────────────
        qs_role = self._create_qs_role(account_id, gold_bucket, results_bucket)

        # ── 3. QuickSight DataSource (Athena) ─────────────────────────────────
        datasource = self._create_datasource(account_id, qs_user_arn)

        # ── 4. QuickSight DataSet (relatorio_vendas) ──────────────────────────
        dataset = self._create_dataset(account_id, datasource, qs_user_arn)

        # ── 5. QuickSight Analysis ────────────────────────────────────────────
        self._create_analysis(account_id, dataset, qs_user_arn)

    # ─────────────────────────────────────────────────────────────────────────
    # 1. Glue
    # ─────────────────────────────────────────────────────────────────────────

    def _create_gold_catalog(self, account_id: str, gold_bucket: str):
        db = glue.CfnDatabase(
            self, "GoldDatabase",
            catalog_id=account_id,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="cloudmart_gold",
                description=(
                    "Data mart de vendas agregado — gerado pelo Glue Job "
                    "silver_to_gold_relatorio_vendas."
                ),
            ),
        )

        crawler_role = iam.Role.from_role_name(self, "GlueEtlRole", "GlueETLRole")

        crawler = glue.CfnCrawler(
            self, "GoldCrawler",
            name="cloudmart-gold-crawler",
            description=(
                "Registra relatorio_vendas (gold) no Glue Catalog "
                "para consulta via Athena e QuickSight."
            ),
            role=crawler_role.role_arn,
            database_name="cloudmart_gold",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{gold_bucket}/relatorio_vendas/",
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="LOG",
                delete_behavior="LOG",
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY",
            ),
        )
        crawler.add_dependency(db)

    # ─────────────────────────────────────────────────────────────────────────
    # 2. IAM Role
    # ─────────────────────────────────────────────────────────────────────────

    def _create_qs_role(
        self,
        account_id: str,
        gold_bucket: str,
        results_bucket: str,
    ) -> iam.Role:
        role = iam.Role(
            self, "QuickSightServiceRole",
            role_name="QuickSightServiceRole",
            assumed_by=iam.ServicePrincipal("quicksight.amazonaws.com"),
            description=(
                "Permite ao QuickSight acessar Athena, S3 (gold + results) "
                "e Glue Catalog (cloudmart_gold)."
            ),
        )

        role.add_to_policy(iam.PolicyStatement(
            sid="AthenaQueryAccess",
            actions=[
                "athena:BatchGetQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:GetQueryResultsStream",
                "athena:ListQueryExecutions",
                "athena:StartQueryExecution",
                "athena:StopQueryExecution",
                "athena:GetWorkGroup",
            ],
            resources=[
                f"arn:aws:athena:{self.region}:{account_id}:workgroup/cloudmart-analysts",
                f"arn:aws:athena:{self.region}:{account_id}:workgroup/primary",
            ],
        ))

        role.add_to_policy(iam.PolicyStatement(
            sid="S3GoldAndResultsAccess",
            actions=["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
            resources=[
                f"arn:aws:s3:::{gold_bucket}",
                f"arn:aws:s3:::{gold_bucket}/*",
                f"arn:aws:s3:::{results_bucket}",
                f"arn:aws:s3:::{results_bucket}/*",
            ],
        ))

        role.add_to_policy(iam.PolicyStatement(
            sid="GlueCatalogRead",
            actions=[
                "glue:GetDatabase", "glue:GetDatabases",
                "glue:GetTable",    "glue:GetTables",
                "glue:GetPartition","glue:GetPartitions",
                "glue:BatchGetPartition",
            ],
            resources=["*"],
        ))

        return role

    # ─────────────────────────────────────────────────────────────────────────
    # 3. DataSource
    # ─────────────────────────────────────────────────────────────────────────

    def _create_datasource(
        self,
        account_id: str,
        qs_user_arn: str,
    ) -> quicksight.CfnDataSource:

        return quicksight.CfnDataSource(
            self, "AthenaDataSource",
            aws_account_id=account_id,
            data_source_id="cloudmart-athena",
            name="CloudMart – Athena",
            type="ATHENA",
            data_source_parameters=quicksight.CfnDataSource.DataSourceParametersProperty(
                athena_parameters=quicksight.CfnDataSource.AthenaParametersProperty(
                    work_group="cloudmart-analysts",
                )
            ),
            ssl_properties=quicksight.CfnDataSource.SslPropertiesProperty(
                disable_ssl=False,
            ),
            permissions=[
                quicksight.CfnDataSource.ResourcePermissionProperty(
                    principal=qs_user_arn,
                    actions=[
                        "quicksight:UpdateDataSourcePermissions",
                        "quicksight:DescribeDataSource",
                        "quicksight:DescribeDataSourcePermissions",
                        "quicksight:PassDataSource",
                        "quicksight:UpdateDataSource",
                        "quicksight:DeleteDataSource",
                    ],
                )
            ],
        )

    # ─────────────────────────────────────────────────────────────────────────
    # 4. DataSet
    # ─────────────────────────────────────────────────────────────────────────

    def _create_dataset(
        self,
        account_id: str,
        datasource: quicksight.CfnDataSource,
        qs_user_arn: str,
    ) -> quicksight.CfnDataSet:

        # Colunas de cloudmart_gold.relatorio_vendas (geradas pelo Glue Job)
        columns = [
            quicksight.CfnDataSet.InputColumnProperty(name="categoria",      type="STRING"),
            quicksight.CfnDataSet.InputColumnProperty(name="nome_regiao",    type="STRING"),
            quicksight.CfnDataSet.InputColumnProperty(name="canal_venda",    type="STRING"),
            quicksight.CfnDataSet.InputColumnProperty(name="ano",            type="INTEGER"),
            quicksight.CfnDataSet.InputColumnProperty(name="mes",            type="INTEGER"),
            quicksight.CfnDataSet.InputColumnProperty(name="total_pedidos",  type="INTEGER"),
            quicksight.CfnDataSet.InputColumnProperty(name="total_unidades", type="INTEGER"),
            quicksight.CfnDataSet.InputColumnProperty(name="receita_bruta",  type="DECIMAL"),
            quicksight.CfnDataSet.InputColumnProperty(name="ticket_medio",   type="DECIMAL"),
            quicksight.CfnDataSet.InputColumnProperty(name="maior_venda",    type="DECIMAL"),
            quicksight.CfnDataSet.InputColumnProperty(name="menor_venda",    type="DECIMAL"),
        ]

        dataset = quicksight.CfnDataSet(
            self, "RelatórioVendasDataSet",
            aws_account_id=account_id,
            data_set_id="cloudmart-relatorio-vendas",
            name="CloudMart – Relatório de Vendas",
            import_mode="DIRECT_QUERY",
            physical_table_map={
                "relatorio-vendas-phys": quicksight.CfnDataSet.PhysicalTableProperty(
                    custom_sql=quicksight.CfnDataSet.CustomSqlProperty(
                        data_source_arn=datasource.attr_arn,
                        name="relatorio_vendas",
                        sql_query="SELECT * FROM cloudmart_gold.relatorio_vendas",
                        columns=columns,
                    )
                )
            },
            logical_table_map={
                "relatorio-vendas-logic": quicksight.CfnDataSet.LogicalTableProperty(
                    alias="relatorio_vendas",
                    source=quicksight.CfnDataSet.LogicalTableSourceProperty(
                        physical_table_id="relatorio-vendas-phys",
                    ),
                )
            },
            permissions=[
                quicksight.CfnDataSet.ResourcePermissionProperty(
                    principal=qs_user_arn,
                    actions=[
                        "quicksight:UpdateDataSetPermissions",
                        "quicksight:DescribeDataSet",
                        "quicksight:DescribeDataSetPermissions",
                        "quicksight:PassDataSet",
                        "quicksight:DescribeIngestion",
                        "quicksight:ListIngestions",
                        "quicksight:UpdateDataSet",
                        "quicksight:DeleteDataSet",
                        "quicksight:CreateIngestion",
                        "quicksight:CancelIngestion",
                    ],
                )
            ],
        )
        dataset.add_dependency(datasource)
        return dataset

    # ─────────────────────────────────────────────────────────────────────────
    # 5. Analysis
    # ─────────────────────────────────────────────────────────────────────────

    def _create_analysis(
        self,
        account_id: str,
        dataset: quicksight.CfnDataSet,
        qs_user_arn: str,
    ) -> quicksight.CfnAnalysis:

        visuals = [
            self._kpi_visual("kpi-receita",   "Receita Total (R$)",           _DS, "receita_bruta", "SUM"),
            self._kpi_visual("kpi-pedidos",   "Total de Pedidos",             _DS, "total_pedidos", "SUM"),
            self._kpi_visual("kpi-ticket",    "Ticket Médio (R$)",            _DS, "ticket_medio",  "AVERAGE"),
            self._bar_visual("bar-categoria", "Receita por Categoria",        _DS, "categoria",    "receita_bruta"),
            self._bar_visual("bar-regiao",    "Receita por Região",           _DS, "nome_regiao",  "receita_bruta"),
            self._line_visual("line-mensal",  "Tendência Mensal de Receita",  _DS, "mes", "receita_bruta", "categoria"),
        ]

        # Layout grid QuickSight — 36 colunas, linhas de 4px cada
        grid_elements = [
            self._grid_elem("kpi-receita",    0,  0, 12, 4),   # linha 0: 3 KPIs
            self._grid_elem("kpi-pedidos",   12,  0, 12, 4),
            self._grid_elem("kpi-ticket",    24,  0, 12, 4),
            self._grid_elem("bar-categoria",  0,  4, 18, 12),  # linha 4: 2 barras
            self._grid_elem("bar-regiao",    18,  4, 18, 12),
            self._grid_elem("line-mensal",    0, 16, 36, 12),  # linha 16: linha de tendência
        ]

        definition = quicksight.CfnAnalysis.AnalysisDefinitionProperty(
            data_set_identifier_declarations=[
                quicksight.CfnAnalysis.DataSetIdentifierDeclarationProperty(
                    data_set_arn=dataset.attr_arn,
                    identifier=_DS,
                )
            ],
            sheets=[
                quicksight.CfnAnalysis.SheetDefinitionProperty(
                    sheet_id="sheet-main",
                    name="Visão Executiva de Vendas",
                    visuals=visuals,
                    layouts=[
                        quicksight.CfnAnalysis.LayoutProperty(
                            configuration=quicksight.CfnAnalysis.LayoutConfigurationProperty(
                                grid_layout=quicksight.CfnAnalysis.GridLayoutConfigurationProperty(
                                    elements=grid_elements,
                                )
                            )
                        )
                    ],
                )
            ],
        )

        analysis = quicksight.CfnAnalysis(
            self, "VendasAnalysis",
            aws_account_id=account_id,
            analysis_id="cloudmart-vendas-analysis",
            name="CloudMart – Visão Executiva de Vendas",
            definition=definition,
            permissions=[
                quicksight.CfnAnalysis.ResourcePermissionProperty(
                    principal=qs_user_arn,
                    actions=[
                        "quicksight:RestoreAnalysis",
                        "quicksight:UpdateAnalysisPermissions",
                        "quicksight:DeleteAnalysis",
                        "quicksight:DescribeAnalysisPermissions",
                        "quicksight:QueryAnalysis",
                        "quicksight:DescribeAnalysis",
                        "quicksight:UpdateAnalysis",
                    ],
                )
            ],
        )
        analysis.add_dependency(dataset)
        return analysis

    # ── Helpers de visuais ────────────────────────────────────────────────────

    def _col(self, ds_id: str, col_name: str):
        return quicksight.CfnAnalysis.ColumnIdentifierProperty(
            data_set_identifier=ds_id, column_name=col_name
        )

    def _cat_dim(self, field_id: str, ds_id: str, col_name: str):
        return quicksight.CfnAnalysis.DimensionFieldProperty(
            categorical_dimension_field=quicksight.CfnAnalysis.CategoricalDimensionFieldProperty(
                field_id=field_id, column=self._col(ds_id, col_name)
            )
        )

    def _num_dim(self, field_id: str, ds_id: str, col_name: str):
        return quicksight.CfnAnalysis.DimensionFieldProperty(
            numerical_dimension_field=quicksight.CfnAnalysis.NumericalDimensionFieldProperty(
                field_id=field_id, column=self._col(ds_id, col_name)
            )
        )

    def _num_measure(self, field_id: str, ds_id: str, col_name: str, agg: str):
        return quicksight.CfnAnalysis.MeasureFieldProperty(
            numerical_measure_field=quicksight.CfnAnalysis.NumericalMeasureFieldProperty(
                field_id=field_id,
                column=self._col(ds_id, col_name),
                aggregation_function=quicksight.CfnAnalysis.NumericalAggregationFunctionProperty(
                    simple_numerical_aggregation=agg
                ),
            )
        )

    def _title(self, text: str):
        return quicksight.CfnAnalysis.VisualTitleLabelOptionsProperty(
            visibility="VISIBLE",
            format_text=quicksight.CfnAnalysis.ShortFormatTextProperty(plain_text=text),
        )

    def _kpi_visual(self, visual_id: str, title: str, ds_id: str, col_name: str, agg: str):
        return quicksight.CfnAnalysis.VisualProperty(
            kpi_visual=quicksight.CfnAnalysis.KPIVisualProperty(
                visual_id=visual_id,
                title=self._title(title),
                chart_configuration=quicksight.CfnAnalysis.KPIConfigurationProperty(
                    field_wells=quicksight.CfnAnalysis.KPIFieldWellsProperty(
                        values=[self._num_measure(f"{visual_id}-v", ds_id, col_name, agg)]
                    )
                ),
            )
        )

    def _bar_visual(self, visual_id: str, title: str, ds_id: str, cat_col: str, val_col: str):
        return quicksight.CfnAnalysis.VisualProperty(
            bar_chart_visual=quicksight.CfnAnalysis.BarChartVisualProperty(
                visual_id=visual_id,
                title=self._title(title),
                chart_configuration=quicksight.CfnAnalysis.BarChartConfigurationProperty(
                    field_wells=quicksight.CfnAnalysis.BarChartFieldWellsProperty(
                        bar_chart_aggregated_field_wells=quicksight.CfnAnalysis.BarChartAggregatedFieldWellsProperty(
                            category=[self._cat_dim(f"{visual_id}-c", ds_id, cat_col)],
                            values=[self._num_measure(f"{visual_id}-v", ds_id, val_col, "SUM")],
                        )
                    ),
                    orientation="HORIZONTAL",
                    sort_configuration=quicksight.CfnAnalysis.BarChartSortConfigurationProperty(
                        category_sort=[
                            quicksight.CfnAnalysis.FieldSortOptionsProperty(
                                field_sort=quicksight.CfnAnalysis.FieldSortProperty(
                                    field_id=f"{visual_id}-v", direction="DESC"
                                )
                            )
                        ],
                        category_items_limit=quicksight.CfnAnalysis.ItemsLimitConfigurationProperty(
                            items_limit=10, other_categories="INCLUDE"
                        ),
                    ),
                ),
            )
        )

    def _line_visual(
        self, visual_id: str, title: str, ds_id: str,
        x_col: str, y_col: str, color_col: str
    ):
        return quicksight.CfnAnalysis.VisualProperty(
            line_chart_visual=quicksight.CfnAnalysis.LineChartVisualProperty(
                visual_id=visual_id,
                title=self._title(title),
                chart_configuration=quicksight.CfnAnalysis.LineChartConfigurationProperty(
                    field_wells=quicksight.CfnAnalysis.LineChartFieldWellsProperty(
                        line_chart_aggregated_field_wells=quicksight.CfnAnalysis.LineChartAggregatedFieldWellsProperty(
                            category=[self._num_dim(f"{visual_id}-x", ds_id, x_col)],
                            values=[self._num_measure(f"{visual_id}-v", ds_id, y_col, "SUM")],
                            colors=[self._cat_dim(f"{visual_id}-c", ds_id, color_col)],
                        )
                    ),
                    type="LINE",
                    sort_configuration=quicksight.CfnAnalysis.LineChartSortConfigurationProperty(
                        category_sort=[
                            quicksight.CfnAnalysis.FieldSortOptionsProperty(
                                field_sort=quicksight.CfnAnalysis.FieldSortProperty(
                                    field_id=f"{visual_id}-x", direction="ASC"
                                )
                            )
                        ]
                    ),
                ),
            )
        )

    def _grid_elem(
        self, visual_id: str,
        col_idx: int, row_idx: int, col_span: int, row_span: int
    ):
        return quicksight.CfnAnalysis.GridLayoutElementProperty(
            element_id=visual_id,
            element_type="VISUAL",
            column_index=col_idx,
            row_index=row_idx,
            column_span=col_span,
            row_span=row_span,
        )
