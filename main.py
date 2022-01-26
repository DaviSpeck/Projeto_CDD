from bancoSQLite import Connect
from bancoMongo import ConnectMongo
from IPython.display import display

import os
import time
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sn

def GerarRelatorioCVS():

    banco = Connect()

    # Definindo relatório geral
    resultado = banco.ler_registros("""
                        SELECT
                        tipomovimento.descricao,tipomovimento.model,
                        coleta.data,coleta.hora,coleta.quantidade,
                        sexo.sexo,sexo.sigla,
                        dados.x,dados.y,dados.z, (dados.x + dados.y + dados.z)/3
                        FROM coleta
                        INNER JOIN tipomovimento ON (tipomovimento.idtipomovimento = coleta.idtipomovimento)
                        INNER JOIN sexo ON (sexo.idsexo = coleta.idsexo)
                        INNER JOIN dados ON (dados.idcoleta = coleta.idcoleta)
                        WHERE tipomovimento.model == 'False';
                        """)

    df = pd.DataFrame(resultado, columns=[
                      "TipoMovimento", "Model", "Data", "Hora", "Quantidade", "Sexo", "Sigla", "X", "Y", "Z", "Media"])

    df.to_csv('Sprint_I/relatorio/RelatorioGeral.csv')

    # Tipo de Movimento
    resultado = banco.ler_registros("""
                        SELECT
                        tipomovimento.descricao,tipomovimento.model
                        FROM
                        tipomovimento;
                        """)

    df = pd.DataFrame(resultado, columns=["TipoMovimento", "Model"])

    df.to_csv('Sprint_I/relatorio/tipomovimento.csv')

    # Tipo de Movimento -->  model true
    resultado = banco.ler_registros("""
                        SELECT
                        tipomovimento.descricao,tipomovimento.model
                        FROM
                        tipomovimento
                        WHERE
                        tipomovimento.model = 'True';
                        """)

    df = pd.DataFrame(resultado, columns=["TipoMovimento", "Model"])

    df.to_csv('Sprint_I/relatorio/tipomovimento_model_true.csv')

    # Tipo de Movimento --> model False
    resultado = banco.ler_registros("""
                        SELECT
                        tipomovimento.descricao,tipomovimento.model
                        FROM
                        tipomovimento
                        WHERE
                        tipomovimento.model = 'False';
                        """)

    df = pd.DataFrame(resultado, columns=["TipoMovimento", "Model"])

    df.to_csv('Sprint_I/relatorio/tipomovimento_model_false.csv')

    # Quantidade --> tipo de movimento
    resultado = banco.ler_registros("""
                        SELECT
                        COUNT(tipomovimento.idtipomovimento)
                        FROM
                        tipomovimento; /* Quantidade de Tipo de Movimentação */
                        """)

    df = pd.DataFrame(resultado, columns=["Quantidade_Tipo_Movimento"])

    df.to_csv('Sprint_I/relatorio/tipomovimento_quantidade.csv')

    # Quantidade --> sexo
    resultado = banco.ler_registros("""
                        SELECT COUNT(sexo.idsexo) FROM sexo; /* Quantidade de sexo*/
                        """)

    df = pd.DataFrame(resultado, columns=["Quantidade_Sexo"])

    df.to_csv('Sprint_I/relatorio/sexo_quantidade.csv')

    # Tipo de movimento --> sexo
    resultado = banco.ler_registros("""
                        SELECT
                        tipomovimento.descricao,sexo.sexo
                        FROM
                        coleta
                        INNER JOIN tipomovimento ON (tipomovimento.idtipomovimento = coleta.idtipomovimento)
                        INNER JOIN sexo ON (coleta.idsexo = sexo.idsexo)
                        GROUP by tipomovimento.descricao,coleta.idsexo;
                        """)

    df = pd.DataFrame(resultado, columns=["TipoMovimento", "Sexo"])

    df.to_csv('Sprint_I/relatorio/tipomovimento_sexo.csv')

    # Coordenadas
    resultado = banco.ler_registros("""
                        SELECT
                        x,y,z
                        FROM
                        dados;""")

    df = pd.DataFrame(resultado, columns=["X", "Y", "Z"])

    df.to_csv('Sprint_I/relatorio/dados_x_y_z.csv')

    # Quantidade de dados por id --> coleta
    resultado = banco.ler_registros("""
                        SELECT COUNT(coleta.idcoleta) FROM coleta;""")

    df = pd.DataFrame(resultado, columns=["Quantidade_Coleta"])

    df.to_csv('Sprint_I/relatorio/coleta_quantidade.csv')

    # Quantidade de dados por id --> dados
    resultado = banco.ler_registros("""
                        SELECT COUNT(dados.iddados) FROM dados;""")

    df = pd.DataFrame(resultado, columns=["Quantidade_Dados"])

    df.to_csv('Sprint_I/relatorio/dados_quantidade.csv')

    # Soma das coordenadas --> X,Y,Z
    resultado = banco.ler_registros("""
                        SELECT SUM(dados.x), SUM(dados.y), SUM(dados.z) from dados;""")

    df = pd.DataFrame(resultado, columns=["X", "Y", "Z"])

    df.to_csv('Sprint_I/relatorio/dados_soma_x_y_z.csv')

    # Quantidade total de dados coletatos --> coleta
    resultado = banco.ler_registros("""
                        SELECT SUM(coleta.quantidade) from coleta;""")

    df = pd.DataFrame(resultado, columns=["QuantidadeVoluntario"])

    df.to_csv('Sprint_I/relatorio/coleta_quantidade_voluntario.csv')

    # Quantidade total de dados coletatos por sexo --> coleta | sexo
    resultado = banco.ler_registros("""
                        SELECT
                        SUM(coleta.quantidade),sexo.sexo,sexo.sigla
                        FROM
                        coleta
                        INNER JOIN sexo on(coleta.idsexo = sexo.idsexo)
                        GROUP BY coleta.idsexo;""")

    df = pd.DataFrame(resultado, columns=["Quantidade", "Sexo", "Sigla"])

    df.to_csv('Sprint_I/relatorio/coleta_quantidade_por_sexo.csv')

    # Coordenadas por tipo de movimento
    resultado = banco.ler_registros("""
                        SELECT
                        SUM(dados.x), SUM(dados.y), SUM(
                            dados.z),tipomovimento.descricao,tipomovimento.model
                        FROM
                        dados
                        INNER JOIN coleta on (dados.idcoleta = coleta.idcoleta)
                        INNER JOIN tipomovimento on (coleta.idtipomovimento = tipomovimento.idtipomovimento)
                        GROUP BY coleta.idtipomovimento; """)

    df = pd.DataFrame(resultado, columns=[
                      "X", "Y", "Z", "tipomovimento", "model"])

    df.to_csv('Sprint_I/relatorio/dados_soma_x_y_z_tipomovimento.csv')

    # Coleta por ano, voluntário (qtd) e tipo de movimento (qtd) --> GERAL
    resultado = banco.ler_registros("""
                        SELECT
                        SUBSTR(coleta.data,1,4) as 'Ano',
                        SUM(coleta.quantidade) as 'QuantidadeVoluntario',
                        COUNT(tipomovimento.descricao) as 'QuantidadeTipoMovimento'
                        FROM
                        coleta
                        INNER JOIN tipomovimento ON (tipomovimento.idtipomovimento = coleta.idtipomovimento)
                        GROUP BY ano;""")

    df = pd.DataFrame(resultado, columns=[
                      "Ano", "QuantidadeVoluntario", "QuantidadeTipoMovimento"])

    df.to_csv(
        'Sprint_I/relatorio/coleta_ano_quantidadevoluntario_quantidadetipomovimento.csv')

    # Coleta por ano, voluntário (qtd) e tipo de movimento (qtd) --> MODEL false
    resultado = banco.ler_registros("""
                        SELECT
                        SUBSTR(coleta.data,1,4) as 'Ano',
                        SUM(coleta.quantidade) as 'QuantidadeVoluntario',
                        COUNT(tipomovimento.descricao) as 'QuantidadeTipoMovimento'
                        FROM
                        coleta
                        INNER JOIN tipomovimento ON (tipomovimento.idtipomovimento = coleta.idtipomovimento)
                        WHERE tipomovimento.model = 'False'
                        GROUP BY ano;""")

    df = pd.DataFrame(resultado, columns=[
                      "Ano", "QuantidadeVoluntario", "QuantidadeTipoMovimento"])

    df.to_csv(
        'Sprint_I/relatorio/coleta_ano_quantidadevoluntario_quantidadetipomovimento_false.csv')

    # Coleta por ano, voluntário (qtd) e tipo de movimento (qtd) --> MODEL true
    resultado = banco.ler_registros("""
                        SELECT
                        SUBSTR(coleta.data,1,4) as 'Ano',
                        SUM(coleta.quantidade) as 'QuantidadeVoluntario',
                        COUNT(tipomovimento.descricao) as 'QuantidadeTipoMovimento'
                        FROM
                        coleta
                        INNER JOIN tipomovimento ON (tipomovimento.idtipomovimento = coleta.idtipomovimento)
                        WHERE tipomovimento.model = 'True'
                        GROUP BY ano;""")

    df = pd.DataFrame(resultado, columns=[
                      "Ano", "QuantidadeVoluntario", "QuantidadeTipoMovimento"])

    df.to_csv(
        'Sprint_I/relatorio/coleta_ano_quantidadevoluntario_quantidadetipomovimento_true.csv')

    # Coleta por ano, mês, voluntário (qtd) e tipo de movimento (qtd) --> GERAL
    resultado = banco.ler_registros("""
                        SELECT
                        SUBSTR(coleta.data,1,4) as 'Ano',
                        SUBSTR(coleta.data,6,2) as 'Mes',
                        SUM(coleta.quantidade) as 'QuantidadeVoluntario',
                        COUNT(tipomovimento.descricao) as 'QuantidadeTipoMovimento'
                        FROM
                        coleta
                        INNER JOIN tipomovimento ON (tipomovimento.idtipomovimento = coleta.idtipomovimento)
                        GROUP BY ano,mes;""")

    df = pd.DataFrame(resultado, columns=[
                      "Ano", "Mes", "QuantidadeVoluntario", "QuantidadeTipoMovimento"])

    df.to_csv(
        'Sprint_I/relatorio/coleta_ano_mes_quantidadevoluntario_quantidadetipomovimento.csv')

    # Coleta por período (Manhã, Tarde, Noite)
    resultado = banco.ler_registros("""
                        SELECT
                        'Manhã' as 'Periodo',
                        1 as 'idperiodo',
                        SUM(coleta.quantidade),
                        sexo.sexo, sexo.sigla
                        FROM coleta
                        INNER JOIN sexo ON (coleta.idsexo = sexo.idsexo)
                        WHERE coleta.hora >= '00:00:00' and coleta.hora <= '11:59:59'
                        GROUP BY sexo.idsexo
                        UNION ALL
                        SELECT
                        'Tarde' as 'Periodo',
                        2 as 'idperiodo',
                        SUM(coleta.quantidade),
                        sexo.sexo, sexo.sigla
                        FROM coleta
                        INNER JOIN sexo ON (coleta.idsexo = sexo.idsexo)
                        WHERE coleta.hora >= '12:00:00' and coleta.hora <= '17:59:59'
                        GROUP BY sexo.idsexo
                        UNION ALL
                        SELECT
                        'Noite' as 'Periodo',
                        3 as 'idperiodo',
                        SUM(coleta.quantidade),
                        sexo.sexo, sexo.sigla
                        FROM coleta
                        INNER JOIN sexo ON (coleta.idsexo = sexo.idsexo)
                        WHERE coleta.hora >= '18:00:00' and coleta.hora <= '23:59:59'
                        GROUP BY sexo.idsexo;""")

    df = pd.DataFrame(resultado, columns=[
                      "Periodo", "IdPeriodo", "Quantidade", "sexo", "sigla"])

    df.to_csv('Sprint_I/relatorio/coleta_periodo.csv')

    resultado = banco.ler_registros("""
                    SELECT 
                    AVG(dados.x), AVG(dados.y), AVG(dados.z),tipomovimento.descricao,tipomovimento.model    
                    FROM 
                    coleta
                    INNER JOIN dados ON (coleta.idcoleta = dados.idcoleta)
                    INNER JOIN tipomovimento ON (coleta.idtipomovimento = tipomovimento.idtipomovimento)
                    GROUP BY tipomovimento.idtipomovimento;
                        """)

    df = pd.DataFrame(resultado, columns=[
                      "X", "Y", "Z", "TipoMovimento","model"])

    df.to_csv('Sprint_I/relatorio/tipomovimento_media_x_y_z.csv')

    resultado = banco.ler_registros("""
                    SELECT 
                    AVG(dados.x), AVG(dados.y), AVG(dados.z),tipomovimento.descricao,tipomovimento.model    
                    FROM 
                    coleta
                    INNER JOIN dados ON (coleta.idcoleta = dados.idcoleta)
                    INNER JOIN tipomovimento ON (coleta.idtipomovimento = tipomovimento.idtipomovimento)
                    GROUP BY tipomovimento.idtipomovimento;
                        """)

    df = pd.DataFrame(resultado, columns=[
                      "X", "Y", "Z", "TipoMovimento","Model"])

    df.to_csv('Sprint_I/relatorio/tipomovimento_soma_media_x_y_z.csv')


    resultado = banco.ler_registros("""                    
            SELECT  
            CASE JULIANDAY(date(data, 'weekday 0')) - JULIANDAY(date(data))
            WHEN 6 THEN 'Segunda-Feira'
            WHEN 5 THEN 'Terça-Feira'
            WHEN 4 THEN 'Quarta-feira'
            WHEN 3 THEN 'Quinta-Feira'
            WHEN 2 THEN 'Sexta-Feira'
            WHEN 1 THEN 'Sabado'
            WHEN 0 THEN 'Domingo'
            ELSE 'Não sei que dia da semana'
            END diasemana,data,tipomovimento.descricao,tipomovimento.model
            FROM coleta
            INNER JOIN tipomovimento ON (coleta.idtipomovimento = tipomovimento.idtipomovimento)
    """)

    df = pd.DataFrame(resultado, columns=[
                      "DiaSemana", "Data", "TipoMovimento", "Model"])

    df.to_csv('Sprint_I/relatorio/tipomovimento_diasemana_data.csv')

    resultado = banco.ler_registros("""                    
            SELECT 
            tipomovimento.descricao,dados.x, dados.y,dados.z    
            FROM coleta
            INNER JOIN dados ON (coleta.idcoleta = dados.idcoleta)
            INNER JOIN tipomovimento ON (coleta.idtipomovimento = tipomovimento.idtipomovimento);
    """)

    df = pd.DataFrame(resultado, columns=[
                      "TipoMovimento", "x", "Y", "z"])

    df.to_csv('Sprint_I/relatorio/tipomovimento_x_y_z.csv')

    resultado = banco.ler_registros("""                    
            SELECT 
            tipomovimento.descricao,dados.x, dados.y,dados.z    
            FROM coleta
            INNER JOIN dados ON (coleta.idcoleta = dados.idcoleta)
            INNER JOIN tipomovimento ON (coleta.idtipomovimento = tipomovimento.idtipomovimento);
    """)

    df = pd.DataFrame(resultado, columns=[
                      "TipoMovimento", "x", "Y", "z"])

    df.to_csv('Sprint_I/relatorio/tipomovimento_x_y_z_false.csv')

    resultado = banco.ler_registros("""                    
            SELECT 
            tipomovimento.descricao,dados.x, dados.y,dados.z    
            FROM coleta
            INNER JOIN dados ON (coleta.idcoleta = dados.idcoleta)
            INNER JOIN tipomovimento ON (coleta.idtipomovimento = tipomovimento.idtipomovimento);
    """)

    df = pd.DataFrame(resultado, columns=[
                      "TipoMovimento", "x", "Y", "z"])

    df.to_csv('Sprint_I/relatorio/tipomovimento_x_y_z_true.csv')

def CorrelacaoCoordenada():

    # Lendo .csv para montagem da correlação
    df = pd.read_csv('Sprint_I/relatorio/tipomovimento_x_y_z.csv')
    df = df.drop(columns=['Unnamed: 0'])
    df['media'] = df.mean(axis=1)
    correlacao = df.corr()

    # Criando correlação --> 4. Procurar alguma correlação entre as coordenadas (valores negativos indicam...
    plot = sn.heatmap(correlacao, annot=True, fmt=".1f", linewidths=.6)

    # Exportanto figura
    sfig = plot.get_figure()
    sfig.set_size_inches(8, 5)
    sfig.savefig('Sprint_I/img/CorrelacaoCoordenada.png')

    # Mostrando correlação
    plt.show()

def HistogramaMedidasX():

    # Lendo .csv para montagem do histograma
    hmp_df = pd.read_csv('Sprint_I/relatorio\Relatoriogeral.csv')

    # Criando histograma --> 5. Monte um histograma revelando a distribuição das medidas obtidas nas coordenadas X;
    x_hist = hmp_df['X'].hist(bins=100, grid=False, color='red')
    x_hist.set_title('Distribuição das coordenadas X')
    x_hist.set_xlabel('Medida')
    x_hist.set_ylabel('Quantidade')

    # Exportanto figura
    sfig = x_hist.get_figure()
    sfig.set_size_inches(8, 5)
    sfig.savefig('Sprint_I/img/HistogramaMedidasX.png')

    # Mostrando histograma
    plt.show()

def GraficoTipoMovimento():

    # Lendo .csv para montagem do gráfico
    df = pd.read_csv('Sprint_I/relatorio\Relatoriogeral.csv')

    # Criando grágico --> 6. Gere um gráfico com ocorrências por tipo de movimento.
    plt.title("Ocorrências por tipo de movimento")
    plt.ylabel('Quantidade')
    plt.xlabel('Tipo de Movimento')

    ax = plt.gca()
    ax.tick_params(axis='x', colors='black')
    ax.tick_params(axis='y', colors='black')

    graf = df.groupby(['TipoMovimento']).TipoMovimento.count().plot(
        kind='bar',
        color='red'
    )

    plt.subplots_adjust(bottom=0.30)

    # Exportando figura
    sfig = graf.get_figure()
    sfig.set_size_inches(8, 5)
    sfig.savefig('Sprint_I/img/GraficoTipoMovimento.png')

    # Mostrando gráfico
    plt.show()

def analiseDiretorio():

    if os.path.isfile('registro.db'):
        os.remove("registro.db")

    banco = Connect()
    bancoMongo = ConnectMongo()

    pasta = './Sprint_I/HMP_Dataset'
    tipomovimento = ''
    model = False
    data = ''
    hora = ''
    sexo = 0
    quantidade = 0
    idtipomovimento = 0
    idcoleta = 0
    registroDadosMongo = ''
    registroDados = ''

    for diretorio, subpastas, arquivos in os.walk(pasta):

        for subpasta in subpastas:

            tipomovimento = subpasta.replace('_MODEL', '')

            if subpasta.find('_MODEL') != -1:
                model = True
            else:
                model = False

            banco.inserir_registro("""
                    INSERT INTO tipomovimento (descricao,model)
                    VALUES ('{}','{}');
                    """.format(tipomovimento, model))

            idtipomovimento = banco.ler_registro(
                'SELECT MAX(idtipomovimento) FROM tipomovimento;')[0]

            for arquivo in os.listdir(os.path.join(pasta, subpasta)):
                for i, valor in enumerate(arquivo.replace('.txt', '').split('-')):
                    if i > 0 and i < 4:
                        data = valor if i == 1 else '{}-{}'.format(data, valor)
                    elif i > 3 and i < 7:
                        hora = valor if i == 4 else '{}:{}'.format(hora, valor)
                    elif i == 8:
                        sexo = 'f' if valor[0] == 'f' else 'm'

                        quantidade = valor[1:].replace('_', '')

                banco.inserir_registro("""
                            INSERT INTO coleta (idsexo,idtipomovimento,data,hora,quantidade)
                            VALUES ({},{},'{}','{}',{});
                    """.format(1 if sexo == 'f' else 2, idtipomovimento, data, hora, quantidade))

                idcoleta = banco.ler_registro(
                    "SELECT MAX(idcoleta) FROM coleta;")[0]

                dados = pd.read_table('{}'.format(os.path.join(
                    pasta, subpasta, arquivo)), sep=" ", header=None, names=["X", "Y", "Z"])

                ini = time.time()
                # valor = ''

                registroDadosMongo = """["""
                
                for i in range(len(dados)):
                    if registroDados:
                        registroDados = '{},({},{},{},{})'.format(
                            registroDados, idcoleta, dados['X'][i], dados['Y'][i], dados['Z'][i])                            
                    else:
                        registroDados = '({},{},{},{})'.format(
                            idcoleta, dados['X'][i], dados['Y'][i], dados['Z'][i])
                    
                    registroDadosMongo = """%s{"descricao": "%s","model": "%s","data": "%s","hora": "%s","quantidade":"%s","sexo":"%s","x":"%s","y":"%s","z":"%s"},""" % (registroDadosMongo,tipomovimento,model,str(data),str(hora),str(quantidade),str(sexo),str(dados['X'][i]),str(dados['Y'][i]),str(dados['Z'][i]))

                registroDadosMongo = """%s ]""" % (registroDadosMongo[:-1])

                bancoMongo.inserir_registro(json.loads(registroDadosMongo))            

                banco.inserir_registro("""
                     INSERT INTO dados (idcosleta,x,y,z)
                     VALUES {};
                     """.format(registroDados))

                registroDados = ''
                registroDadosMongo = ''

                fim = time.time()
                print("Tipo de Movimento {} para o arquivo {} levou {} para se executado: ".format(
                     tipomovimento, os.path.join(pasta, subpasta, arquivo), fim-ini))   
    
            
            

    input('tudo certo')    

def main():

    # aList = [{'a':1, 'b':2}, {'c':3, 'd':4}]
    # aList.append([{'e':5,'f':6}])

    # jsonStr = json.dumps(aList)
    # print(aList)
    # print(jsonStr)

    opt = 1
    while (opt <= 7 or opt >= 1):

        # Menu de opções
        print('Digite uma das opcoes abaixo')
        print('1 - Analisa Diretorio HMP_Dataset')
        print('2 - Gerar Relatorios do tipo cvs')
        print('3 - Relatorio geral')
        print('4 - Correlacao entre as coordenadas')
        print('5 - Histograma revelando a distribuicao das medidas obtidas nas coordenadas X')
        print('6 - Grafico com ocorrencias por tipo de movimento')
        print('7 - Finalizar programa')

        opt = int(input('Opcao escolhida:'))

        if opt == 1:
            analiseDiretorio()
        elif opt == 2:
            if os.path.isfile('registro.db'):
                GerarRelatorioCVS()
            else:
                print('Não existe o banco de dados')
                print('Por favor gere o banco de dados na opção 1')
                print('Tecle enter para volta ao menu.')
                input('')
        elif opt == 3:
            if os.path.isfile('registro.db'):
    
                df = pd.read_csv('Sprint_I\\relatorio\Relatoriogeral.csv')
                df = df.drop(columns=['Unnamed: 0'])
                display(df)
                print('Tecle enter para volta ao menu.')
                input('')
            else:
                print('Não existe o arquivo relatoriogeral.cvs')
                print('Por favor gere o arquivo na opção 2')
                print('Tecle enter para volta ao menu.')
                input('')
        elif opt == 4:
            if os.path.isfile('Sprint_I/relatorio/tipomovimento_x_y_z.csv'):
                CorrelacaoCoordenada()
            else:
                print('Não existe o arquivo tipomovimento_x_y_z.cvs')
                print('Por favor gere o arquivo na opção 2')
                print('Tecle enter para volta ao menu.')
                input('')
        elif opt == 5:
            if os.path.isfile('Sprint_I/relatorio/Relatoriogeral.csv'):
                HistogramaMedidasX()
            else:
                print('Não existe o arquivo Relatoriogeral.cvs')
                print('Por favor gere o arquivo na opção 2')
                print('Tecle enter para volta ao menu.')
                input('')
        elif opt == 6:
            if os.path.isfile('Sprint_I/relatorio/Relatoriogeral.csv'):
                GraficoTipoMovimento()
            else:
                print('Não existe o arquivo relatoriogeral.cvs')
                print('Por favor gere o arquivo na opção 2')
                print('Tecle enter para volta ao menu.')
                input('')
        elif opt == 7:
            print('Saindo do programa!')
            return
        elif opt > 7 or opt < 1:
            print('Opcao inválida!')

        os.system('cls')

    os.system('cls')

if __name__ == '__main__':
    ini = time.time()
    main()
    fim = time.time()
    print("Tempo de execução: ", fim-ini)