"""
Stack CDK: Controle de Acesso Granular — Lake Formation (LGPD).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CENÁRIO IMPLEMENTADO
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Perfil: Analista de Marketing (IAM Role MarketingAnalystRole)

  ✅ PODE fazer SELECT em cloudmart_bronze.clientes
  ✅ PODE ver todas as colunas: id_cliente, nome_cliente, email,
       segmento, estado, id_regiao, data_cadastro, ingestion_ts
  ❌ NÃO vê a coluna `cpf` — retorna NULL via Lake Formation
  ❌ NÃO tem acesso direto ao S3 bronze (sem PutObject/GetObject)
  ❌ NÃO pode acessar silver ou gold sem concessão adicional

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MECANISMO LAKE FORMATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. CfnResource
   Registra s3://bronze-zone-{account} no Lake Formation.
   Após o registro, o Lake Formation emite credenciais temporárias
   (vended credentials) em vez de usar as permissões IAM do caller
   diretamente no S3. Isso garante que o column filter seja aplicado
   mesmo se o analista tentar acessar o bucket via SDK/CLI.

2. CfnDataCellsFilter  "marketing_analyst_cpf_exclusion"
   Escopo: cloudmart_bronze.clientes | todas as linhas
   Colunas: WILDCARD com exclusão explícita de `cpf`
   → Quando o analista executa SELECT * o Glue/Athena omite `cpf`
     da resposta antes de devolver os dados.

3. CfnPrincipalPermissions
   a) DESCRIBE em cloudmart_bronze → analista vê o banco no console
   b) DESCRIBE em cloudmart_bronze.clientes → vê a tabela no schema
   c) SELECT (data cells filter) → pode consultar sem ver `cpf`

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
JUSTIFICATIVA LGPD
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Art. 6°, III  — Necessidade: limitação ao mínimo necessário para a
               finalidade. Marketing precisa de segmento/estado/email
               para campanhas, não precisa de CPF.
Art. 46        — Medidas técnicas e administrativas aptas a proteger
               dados pessoais de acessos não autorizados.
Art. 49        — Os sistemas devem ser estruturados para atender à
               segurança desde o projeto (privacy by design).
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_lakeformation as lf,
)
from constructs import Construct

# Banco e tabela alvo desta política
_BRONZE_DB    = "cloudmart_bronze"
_CLIENTS_TABLE = "clientes"
_FILTER_NAME  = "marketing_analyst_cpf_exclusion"


class AccessControlStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        account_id    = self.node.try_get_context("account_id") or self.account
        bronze_bucket = f"bronze-zone-{account_id}"

        # ── 1. Registra o bucket bronze no Lake Formation ─────────────────────
        # Necessário para que o LF emita vended credentials (sem acesso S3 direto).
        s3_registration = lf.CfnResource(
            self, "BronzeS3LfRegistration",
            resource_arn=f"arn:aws:s3:::{bronze_bucket}",
            use_service_linked_role=True,  # cria AWSServiceRoleForLakeFormationDataAccess
        )

        # ── 2. IAM Role — Analista de Marketing ──────────────────────────────
        marketing_role = self._create_marketing_analyst_role(account_id)

        # ── 3. Data Cells Filter — exclui coluna cpf ─────────────────────────
        #
        # column_wildcard com excluded_column_names faz o Lake Formation
        # omitir a coluna `cpf` de qualquer query feita por quem usa este filter.
        # O Athena recebe o resultado já sem a coluna — NULL nunca é lido do S3.
        data_filter = lf.CfnDataCellsFilter(
            self, "MarketingAnalystCpfFilter",
            database_name=_BRONZE_DB,
            table_name=_CLIENTS_TABLE,
            name=_FILTER_NAME,
            table_catalog_id=account_id,
            column_wildcard=lf.CfnDataCellsFilter.ColumnWildcardProperty(
                excluded_column_names=["cpf"],
            ),
            # row_filter sem critério → todas as linhas visíveis
            row_filter=lf.CfnDataCellsFilter.RowFilterProperty(
                all_rows_wildcard={},
            ),
        )
        # O filter depende do S3 estar registrado para funcionar corretamente
        data_filter.add_dependency(s3_registration)

        # ── 4. Permissões Lake Formation ──────────────────────────────────────
        self._grant_database_describe(marketing_role, account_id)
        self._grant_table_describe(marketing_role, account_id)
        self._grant_select_with_filter(marketing_role, account_id, data_filter)

    # ─────────────────────────────────────────────────────────────────────────
    # IAM Role
    # ─────────────────────────────────────────────────────────────────────────

    def _create_marketing_analyst_role(self, account_id: str) -> iam.Role:
        """
        Role assumida pelos analistas de marketing.
        Pode usar Athena + ler o Glue Catalog.
        NÃO tem acesso direto ao S3 bronze (Lake Formation controla isso).
        """
        role = iam.Role(
            self, "MarketingAnalystRole",
            role_name="MarketingAnalystRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("athena.amazonaws.com"),
                # Em produção, substituir/complementar pelo ARN do IdP/SSO
                iam.AccountPrincipal(account_id),
            ),
            description="Analista de Marketing — SELECT em clientes sem CPF (LGPD)",
        )

        # Athena — executar queries e recuperar resultados
        role.add_to_policy(iam.PolicyStatement(
            sid="AthenaQueryExecution",
            actions=[
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
                "athena:GetWorkGroup",
                "athena:ListWorkGroups",
            ],
            resources=["*"],
        ))

        # S3 — bucket de resultados do Athena (criado pela AthenaStack)
        role.add_to_policy(iam.PolicyStatement(
            sid="AthenaResultsBucketAccess",
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
                "s3:GetBucketLocation",
            ],
            resources=[
                f"arn:aws:s3:::athena-results-cloudmart-{account_id}",
                f"arn:aws:s3:::athena-results-cloudmart-{account_id}/*",
            ],
        ))

        # Athena — acesso explícito ao workgroup cloudmart-analysts
        role.add_to_policy(iam.PolicyStatement(
            sid="AthenaWorkgroupAccess",
            actions=[
                "athena:GetWorkGroup",
                "athena:ListNamedQueries",
                "athena:GetNamedQuery",
            ],
            resources=[
                f"arn:aws:athena:{self.region}:{account_id}:workgroup/cloudmart-analysts",
            ],
        ))

        # Glue Catalog — leitura de metadados (schema, partições)
        role.add_to_policy(iam.PolicyStatement(
            sid="GlueCatalogRead",
            actions=[
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
            ],
            resources=["*"],
        ))

        # Lake Formation — obter credenciais temporárias (vended credentials)
        # para acessar os dados via Athena sem permissão S3 direta
        role.add_to_policy(iam.PolicyStatement(
            sid="LakeFormationGetDataAccess",
            actions=[
                "lakeformation:GetDataAccess",
            ],
            resources=["*"],
        ))

        return role

    # ─────────────────────────────────────────────────────────────────────────
    # Concessões Lake Formation
    # ─────────────────────────────────────────────────────────────────────────

    def _grant_database_describe(self, role: iam.Role, account_id: str):
        """DESCRIBE no banco → analista vê a lista de tabelas no console."""
        lf.CfnPrincipalPermissions(
            self, "MarketingDatabaseDescribe",
            principal=lf.CfnPrincipalPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=role.role_arn,
            ),
            resource=lf.CfnPrincipalPermissions.ResourceProperty(
                database=lf.CfnPrincipalPermissions.DatabaseResourceProperty(
                    catalog_id=account_id,
                    name=_BRONZE_DB,
                ),
            ),
            permissions=["DESCRIBE"],
            permissions_with_grant_option=[],
        )

    def _grant_table_describe(self, role: iam.Role, account_id: str):
        """DESCRIBE na tabela → analista vê o schema (colunas e tipos)."""
        lf.CfnPrincipalPermissions(
            self, "MarketingTableDescribe",
            principal=lf.CfnPrincipalPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=role.role_arn,
            ),
            resource=lf.CfnPrincipalPermissions.ResourceProperty(
                table=lf.CfnPrincipalPermissions.TableResourceProperty(
                    catalog_id=account_id,
                    database_name=_BRONZE_DB,
                    name=_CLIENTS_TABLE,
                ),
            ),
            permissions=["DESCRIBE"],
            permissions_with_grant_option=[],
        )

    def _grant_select_with_filter(
        self,
        role: iam.Role,
        account_id: str,
        data_filter: lf.CfnDataCellsFilter,
    ):
        """
        SELECT na tabela clientes usando o Data Cells Filter.
        O filter exclui a coluna `cpf` — o analista nunca acessa o dado real.
        """
        grant = lf.CfnPrincipalPermissions(
            self, "MarketingSelectWithFilter",
            principal=lf.CfnPrincipalPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=role.role_arn,
            ),
            resource=lf.CfnPrincipalPermissions.ResourceProperty(
                data_cells_filter=lf.CfnPrincipalPermissions.DataCellsFilterResourceProperty(
                    database_name=_BRONZE_DB,
                    table_name=_CLIENTS_TABLE,
                    table_catalog_id=account_id,
                    name=_FILTER_NAME,
                ),
            ),
            permissions=["SELECT"],
            permissions_with_grant_option=[],
        )
        # A concessão só é válida depois que o filter existir
        grant.add_dependency(data_filter)
