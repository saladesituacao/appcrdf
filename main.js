const num = process.env.NUM || 500;
const tabela = process.env.TABLE || 'st_stick.tb_solicitacao_ambulatorial';
const url = process.env.URL || 'https://user:senha@sisab-kb.saude.gov.br:443';
const conexao = process.env.CONECTION || 'user=st_stick host= dbname= password= port=';
var index_solicitacao = process.env.INDEX || 'sisreg-solicitacao-ambulatorial-df';


var elasticsearch = require('elasticsearch');
var fs = require('fs');
var Client = require('pg-native');



var client = new elasticsearch.Client({
    host: url,
});

var clientsql = new Client()
const update = `update  ${tabela} set timestamp=$2, info=$3 where id=$1`;
const insert = `INSERT INTO ${tabela} (id, timestamp, info) values ( $1, $2, $3)`;
const select = `select id from ${tabela} where id=$1`;
const selectmax = `select max(timestamp) from ${tabela}`;

clientsql.connectSync(conexao);
clientsql.prepareSync('update', update, 3);
clientsql.prepareSync('insert', insert, 3);
clientsql.prepareSync('select', select, 1);
clientsql.prepareSync('selectmax', selectmax, 0);


let scroll = 1;
let totalRegistros = 0;
let totalRegistroBaixados = 0;

async function escreveBanco(resultado) {
    var size = resultado.hits.hits.length;

    for(var k=0; k<size; k++){  
        item =  resultado.hits.hits[k];
        //var res = await clientsql.querySync(`select id from sisreg.solicitacao_ambulatorial where id=${item._id}`);
        var res = clientsql.executeSync('select', [item._id]);
        clientsql.executeSync(res.length==0?'insert':'update', [item._id, item._source['@timestamp'], JSON.stringify(item._source)])
    }
}

async function buscaUltimo(){
    var res = clientsql.executeSync('selectmax');
    var ans = 0;

    if(res && res.length>0){
        ans = res[0].max;
    }

    return ans;
}


async function load(){
    
    var ultimo = await buscaUltimo();
    if(ultimo && ultimo.length>10)
        ultimo = ultimo.substring(0,10);
    console.log('Dt Ult Reg==>', ultimo);

    var query = {
        query: {
            range: {
                "@timestamp": {
                    "gte" : ultimo
                }
            }
        },
        sort: [ {"@timestamp" : "asc"} ]
    }

    var resultado = await client.search({
        index: index_solicitacao,
        body: query,
        scroll: '1m',
        size: num
    });

    totalRegistros = resultado.hits.total;
    totalRegistroBaixados += resultado.hits.hits.length;

    console.log('Scroll ' + scroll 
        + ' || Registros baixados: ' +  totalRegistroBaixados
        + ' || Registros total: ' +  totalRegistros);

    await escreveBanco(resultado);
    
    while(resultado._scroll_id) {
        scroll++;

        resultado = await client.scroll({
            scrollId: resultado._scroll_id,
            scroll: '30s'
        })
        
        totalRegistroBaixados += resultado.hits.hits.length;

        console.log('Scroll ' + scroll + ' || Registros baixados: ' +  totalRegistroBaixados);

        
        if(totalRegistroBaixados < totalRegistros) {
            await escreveBanco(resultado);
        } else {
            await escreveBanco(resultado);
            break;
        }
    }
}

load();
