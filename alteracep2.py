import pyodbc
import urllib
import urllib.request
import json
import time
import re
import pandas as pd
import io
import os
import shutil
import tabula
import enviroment

def lambda_handler(event, context):

    if 'cep' in event and _regex(event['cep']):
        return json.loads(urllib.request.urlopen(_get_url_api(event['cep'])).read())
    return json.loads("{\"erro\": true, \"mensagem\": \"Formato incorreto\"}")

def _get_url_api(cep):
    return ('http://www.viacep.com.br/ws/{}/json'.format(_replace(cep)))
    
def _replace(str):
    return str.replace("-", "").replace(" ", "")
    
def _regex(str):
    return re.match('[0-9]{8}', _replace(str))

def _get_url_api_2(estado, cidade, logra):
    return ('http://www.viacep.com.br/ws/{}/{}/{}/json'.format(estado, cidade, logra))

def conectar_com_banco(usuario):
    if usuario in 'treinamento':
        server = '192.168.0.141' 
        database = 'academico' 
        username = enviroment.username 
        password = enviroment.password 
    elif usuario in 'producao':
        server = '192.168.0.31' 
        database = 'academico'
        username = enviroment.username 
        password = enviroment.password 
    else:
        print('funcao_nao_encontrado')
    import pyodbc
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cur=cnxn.cursor()
    return(cur)

def lerPDF(pdfName):
    #read_pdf = PyPDF4.PdfFileReader('PDF/'+pdfName)
    tabula.convert_into('PDF/'+pdfName, "output.csv", output_format="csv", pages='all')
    dados2 = pd.read_csv('output.csv', sep=',', encoding='latin1')
    
    return dados2

def getExtension(name):
    fileName, fileExtension = os.path.splitext(name)
    return fileExtension

def isExtensionPermited(extension):
    extensions = ['pdf']
    for x in extensions:
        if extension[:1] == '.':
            if extension[1:].lower() == x:
                return True
            elif extension.lower() == x:
                return True
    return False

def lookupDirectory(path):
    arquivos = []
    if os.path.isdir(path):
        files = os.listdir(path)
        for i in files:
            if isExtensionPermited(getExtension(i)) == True:
                arquivos.append(i)
    return arquivos

def movePDF(arquivo):
    try:
        shutil.move(str('PDF/'+arquivo),str('PDF/Concluidos/'+arquivo))
    except:
        os.mkdir('PDF/Concluidos/')
        shutil.move(str('PDF/'+arquivo),str('PDF/Concluidos/'+arquivo))
    return (arquivo)
    
def executarABagaca(banco,pasta):
    
    arquivosPLeitura = lookupDirectory(pasta) 
    for i in arquivosPLeitura:
        print(i)
    
    for j in arquivosPLeitura:
        dados2=lerPDF(j)
        cursor=conectar_com_banco(banco)
        cursor2=conectar_com_banco(banco)
        #cursor3=conectar_com_banco(banco)
        contador = 0
        contador2 = 0
        for i in(dados2['Id. na escola'].loc[dados2['Campo']=='"41 - CEP"']):
            consulta = """
                    select estado, 
                            cidade, 
                            rtrim(substring(logra,0,10)), 
                            matricula, 
                            id_pessoa
                    from endereco_matricula 
                    where matricula = """ +'\'' +"{0:.0f}".format(i)+'\''
            #print(consulta)
            cursor.execute(consulta)

            row = cursor.fetchone()   
            
            try:
                estado = str(row[0])
                cidade = str(row[1]).replace(' ', '%20')
                logra = str(row[2]).replace('.','').replace(' ', '%20')
                nu_matricula = str(row[3])
                id_pessoa = str(row[4])

                url2 = _get_url_api_2(estado, cidade, logra)
            except:
                KeyError
                continue

            try:
                f = urllib.request.urlopen(url2)
                time.sleep(2)
            except:
                KeyError
                continue
            try:
                conteudo2 = json.loads(str(f.read().decode('utf-8')))
            except:
                continue
            if len(conteudo2)!=0:
                consulta2=('update end_endereco set end_nu_cep= '+conteudo2[0].get('cep').replace('-','')+', end_nm_logradouro='+ '\''+conteudo2[0].get('logradouro')+'\''+', end_nm_bairro='+'\''+conteudo2[0].get('bairro').replace('\'','')+'\'' + ' where end_id_pessoa= '+ id_pessoa)
                #print(consulta2)
                cursor2.execute(consulta2)
                cursor2.commit()
                print('Atualização realizada id:',nu_matricula)
                contador+=1
            #if len(conteudo2)!=0:
               # print('update end_endereco set end_nu_cep= '+conteudo2[0].get('cep').replace('-','')+', end_nm_logradouro='+ '\''+conteudo2[0].get('logradouro').replace('\'','\'\'')+'\''+', end_nm_bairro='+'\''+conteudo2[0].get('bairro')+'\'' + ' where end_id_pessoa= '+ id_pessoa)

            f.close()
            row = cursor.fetchone()
            contador2+=1
    
        print('Arquivo movido: ',movePDF(j)) #descomentar para mover o arquivo
        print('Registros atualizados:', contador, 'de', contador2)
    cursor.close()
    cursor2.close()    
