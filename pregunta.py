"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    with open ('clusters_report.txt', 'r') as file:
        data = file.readlines()
    #información de las columnas
    indice_columna = [i for i in range(len(data)) if '--\n' in data[i] ]
    info_columna = data[0:max(indice_columna)-1]
    info_columna = [i.replace('\n','') for i in info_columna]
    indice_upper = [i for i in range(len(info_columna[0])) if info_columna[0][i].isupper()]
    columnas1 = [info_columna[0][indice_upper[i]:indice_upper[i+1]].strip().replace(' ','_')  if i!=len(indice_upper)-1 else info_columna[0][indice_upper[i]:] for i in range(len(indice_upper))]
    columnas2 =[info_columna[1][indice_upper[i]:indice_upper[i+1]].strip().replace(' ','_')  if i!=len(indice_upper)-1 else info_columna[1][indice_upper[i]:] for i in range(len(indice_upper))]
    columnas = [i.lower() for i in  [columnas1[i].strip().replace(' ','_')+'_'+columnas2[i].strip() if columnas2[i]!='' else columnas1[i].strip().replace(' ','_') for i in range(len(columnas1))] ]
    
    #información de las filas
    data = data[max(indice_columna):]
    indices_filas = [i for i in range(len(data)) if data[i]=='\n' or '-\n' in data[i]]
    info_filas = [data[indices_filas[i]:indices_filas[i+1]] if i !=len(indices_filas)-1 else data[indices_filas[i]:] for i in range(len(indices_filas))]
    info_filas = [n for i in info_filas for n in i if n!='\n']
    info_filas = info_filas[1:]

    posicion_sep_colum = [i for i in range(2,len(info_filas[0])) if info_filas[0][i]!=' ' and info_filas[0][i-1]==' ' and info_filas[0][i-2]==' ']
    lista_filas = []
    for n in  info_filas:
      lista_filas.append([n[posicion_sep_colum[i]:posicion_sep_colum[i+1]] if i!=len(posicion_sep_colum)-1 else n[posicion_sep_colum[i]*bool(i):] for i in range(len(posicion_sep_colum))])
    posicion_sep_fila = [i for i in range(len(lista_filas)) if lista_filas[i][0][0]!=' ']
    filas = []
    for i in range(len(posicion_sep_fila)):
      if i != len(posicion_sep_fila)-1:
        lista = lista_filas[posicion_sep_fila[i]:posicion_sep_fila[i+1]]
        c1 = int(''.join([i[0] for i in lista]).replace('\n','').replace('%','').replace('  ',' ').strip())
        c2 = int(''.join([i[1] for i in lista]).replace('\n','').replace('%','').replace('  ',' ').strip())
        c3 = float(''.join([i[2] for i in lista]).replace('\n','').replace('%','').replace('  ',' ').strip().replace(',','.'))
        c4 = ''.join([i[3] for i in lista]).replace('\n',' ').replace('  ',' ').replace('  ',' ').replace('.','').strip()
        filas.append([c1,c2,c3,c4])
      else:
        lista = lista_filas[posicion_sep_fila[i]:]
        c1 = int(''.join([i[0] for i in lista]).replace('\n','').replace('%','').replace('  ',' ').strip())
        c2 = int(''.join([i[1] for i in lista]).replace('\n','').replace('%','').replace('  ',' ').strip())
        c3 = float(''.join([i[2] for i in lista]).replace('\n','').replace('%','').replace('  ',' ').strip().replace(',','.'))
        c4 = ''.join([i[3] for i in lista]).replace('\n',' ').replace('  ',' ').replace('  ',' ').replace('.','').strip()
        filas.append([c1,c2,c3,c4]) 
    
    df = pd.DataFrame(filas, columns=columnas)
    return df
