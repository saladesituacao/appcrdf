appcrdf
===============

aplicativo em nodejs para carga de dados da regulação a partir do elasticsearch do ministério da saúde


** Uso:

`TABLE=<schema.tabela> INDEX=<nome do indice> node main`

** Exemplo:

`TABLE=st_stick.tb_solicitacao_ambulatorial INDEX=sisreg-solicitacao-ambulatorial-df node main`

** Variaveis de ambiente

NUM = Numero de registros buscados
TABLE = Tabela do banco a ser gravada (schema.tabela)
URL = URL de busca dos dados (indicd do elastic do MS)
CONECTION = Conexao da base de dados
INDEX = Nome do indice do elastic