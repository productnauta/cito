# Segurança — CITO poc-v-d33

## Riscos atuais
- Credenciais de MongoDB e LLM em texto claro nos arquivos de configuração.
- Execução de subprocessos a partir da UI sem autenticação.

## Recomendações
- Migrar credenciais para variáveis de ambiente ou vault.
- Rotacionar chaves expostas.
- Adicionar autenticação e autorização na UI.
- Restringir permissões do banco por usuário/role.
- Revisar logs para evitar exposição de dados sensíveis.
