#Importando bibliotecas necessarias
import json
import csv
import requests
from requests.exceptions import HTTPError
import pandas as pd
import geopandas
import geopy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import zipfile
import os
import shutil
import matplotlib.pyplot as plt
from plotly.io import write_image
import seaborn as sns


#Importando o dado a partir do link
try:
 datasets = requests.get("https://raw.githubusercontent.com/andre-marcos-perez/ebac-course-utils/main/dataset/deliveries.json")
 datasets.raise_for_status()
except HTTPError as exc:
 print(exc)

with open("deliveries.json", "wb") as file:
    file.write(datasets.content)


#Lendo o dado
with open("deliveries.json", mode="r", encoding="utf8") as file:
    data = json.load(file)


  #Criando um dataframe e passando o DATA como sendo a origem dos dados
  deliveries_df = pd.DataFrame(data)

  #Flatten
  hub_origem_df = pd.json_normalize(deliveries_df["origin"])

  #Unindo os dataframes
  deliveries_df = pd.merge(left=deliveries_df,right=hub_origem_df, how="inner", left_index=True, right_index=True)

  #Removendo colunas desnecessarias e renomeando as colunas novas
  deliveries_df = deliveries_df.drop("origin",axis=1)

  deliveries_df = deliveries_df[["name", "region", "lng", "lat", "vehicle_capacity", "deliveries"]]

  deliveries_df.rename(columns={"lng":"hub_lng", "lat":"hub_lat"}, inplace=True)

  #Tratamento de uma lista de dicionarios, vamos utilizar a operação EXPLOSED
  #Transforma cada elemento da lista em uma linha
  deliveries_exploded_df = deliveries_df[["deliveries"]].explode("deliveries")

  #Criamos um dataframe para cada dado que vamos extrair e concatenamos e um dataframe só
  #Usamos o lambda para aplicar um filtro e extraiz um dado de cada vez
  deliveries_normalized_df = pd.concat([
  pd.DataFrame(deliveries_exploded_df["deliveries"].apply(lambda x: x["size"])).rename(columns={"deliveries":"delivery_size"}),
  pd.DataFrame(deliveries_exploded_df["deliveries"].apply(lambda x: x["point"]["lng"])).rename(columns={"deliveries":"delivery_lng"}),
  pd.DataFrame(deliveries_exploded_df["deliveries"].apply(lambda x: x["point"]["lat"])).rename(columns={"deliveries":"delivery_lat"})
  ],axis=1)

  #Removendo a coluna com os dados que normalizamos do dataframe original
  deliveries_df = deliveries_df.drop("deliveries",axis=1)

  #Unificando os dataframes
  deliveries_df = pd.merge(left=deliveries_df, right=deliveries_normalized_df, how="right", left_index=True, right_index=True)
  deliveries_df.reset_index(inplace=True,drop=True)


#Enriquecedo o dataframe - dados dos hubs

#Selecionando colunasdo hub e removendo duplicatas
hub_df = deliveries_df[["region", "hub_lng", "hub_lat"]].drop_duplicates().sort_values(by="region").reset_index(drop=True)
#Criando objeto "geolocator" para informar quem está fazendo a solicitação
geolocator = Nominatim(user_agent="proj_geocoder")
#Criando objeto "geocoder" e intanciando a classe geolocator.reverse que transforma coordenada em dados textuais
geocoder = RateLimiter(geolocator.reverse, min_delay_seconds=1)
#Extrai as coordenadas e aplica o método dentro do objeto geocoder
hub_df["coordinates"] = hub_df["hub_lat"].astype(str) + ", "
hub_df["coordinates"] = hub_df["coordinates"] + hub_df["hub_lng"].astype(str)
hub_df["geodata"] = hub_df["coordinates"].apply(geocoder)
#Separar em coluna os dados dentro de "geodata"
hub_geodata_df = pd.json_normalize(hub_df["geodata"].apply(lambda data: data.raw))
#Selecionar colunas desejadas
hub_geodata_df = hub_geodata_df[["display_name", "address.city"]]
#Colocando as informações dentro de "display_name" em colunas
address_suburb = hub_geodata_df["display_name"].str.split(",", expand=True)
#Selecionando a coluna desejada
address_suburb = address_suburb[3]
#Mesclando a series ao dataframe
address_suburb = pd.Series(data=address_suburb)
hub_geodata_df ["address.suburb"] = address_suburb
#Removendo coluna que utilizamos para obter os dados
hub_geodata_df =hub_geodata_df.drop("display_name", axis=1)
#Completando o dado faltante com o dado ao lado
hub_geodata_df = hub_geodata_df.fillna(hub_geodata_df["address.suburb"].loc[0])
#Concatenando os dataframes hub_df e hub_geodata_df
hub_df = pd.concat([hub_df,hub_geodata_df], axis=1).drop(["coordinates", "geodata"], axis=1)
#Fazendo um .merge entre os dataframes deliveries_df e hub_df
deliveries_df = pd.merge(left=deliveries_df,right=hub_df, how="inner")
#Reordenando as colunas
deliveries_df = deliveries_df[["name", "region", "hub_lng", "hub_lat", "address.city", "address.suburb", "vehicle_capacity", "delivery_size", "delivery_lng", "delivery_lat"]]


#Baixando os dados dos pontos de entrega
try:
 datasets = requests.get("https://raw.githubusercontent.com/andre-marcos-perez/ebac-course-utils/main/dataset/deliveries-geodata.csv")
 datasets.raise_for_status()
except HTTPError as exc:
 print(exc)
#Normalizando o dataset
linhas = datasets.text.strip().split('\n')
dados = [linha.split(',') for linha in linhas]
#Salvando o dado em formato .csv
with open("geodata.csv", 'w', newline='', encoding='utf-8') as arquivo_csv:
    escritor_csv = csv.writer(arquivo_csv)
    escritor_csv.writerows(dados)
#Importando o arquivo .csv
deliveries_geo_df = pd.read_csv("geodata.csv")
geo_deliveries = pd.DataFrame(deliveries_geo_df)
#Renomeando colunas geo
geo_deliveries ["lng_geo"] = geo_deliveries ["delivery_lng"]
geo_deliveries ["lat_geo"] = geo_deliveries ["delivery_lat"]
geo_deliveries = geo_deliveries.drop(["delivery_lng", "delivery_lat"], axis=1)
#Mesclando geo_deliveries com o dataframe original
deliveries_df = pd.concat([deliveries_df,geo_deliveries], axis=1).drop(["lng_geo", "lat_geo"], axis=1)


#Vizualização dos dados por mapas

#Fazendo o dowloud dos shp files(dados do mapa)
URL = "https://geoftp.ibge.gov.br/cartas_e_mapas/bases_cartograficas_continuas/bc100/go_df/versao2016/shapefile/bc100_go_df_shp.zip"

#Criando uma pasta
nome_da_pasta = 'maps'

try:
    # Verifica se a pasta já existe
    if not os.path.exists(nome_da_pasta):
        # Cria a pasta vazia
        os.makedirs(nome_da_pasta)
        print(f"Pasta '{nome_da_pasta}' criada com sucesso!")
    else:
        print(f"A pasta '{nome_da_pasta}' já existe.")
except OSError as exc:
    print(f"Ocorreu um erro ao criar a pasta: {exc}")
# Faz o download do arquivo ZIP
try:
 datasets = requests.get(URL)
 datasets.raise_for_status()
except HTTPError as exc:
 print(exc)
#Extrair os arquivos para a pasta "mapas"
with open("distrito-federal.zip", "wb") as f:
    f.write(datasets.content)
#Salvarna pasta mapas
with zipfile.ZipFile("distrito-federal.zip", "r") as zip_ref:
    zip_ref.extractall("./maps")
# Caminho de origem do arquivo .shp
def extract_file(path_file_src, path_file_dst):
  shutil.copyfile(path_file_src, path_file_dst)
#Extraindo os arquivos .shp e .shx e salvando fora da pasta principal
extract_file("./maps/LIM_Unidade_Federacao_A.shp", "./distrito-federal.shp")
extract_file("./maps/LIM_Unidade_Federacao_A.shx", "./distrito-federal.shx")
#
mapa = geopandas.read_file("distrito-federal.shp")
mapa = mapa.loc[[0]]


#Selecionando os pontos dos hub - mapa hub
geo_hub_df = geopandas.GeoDataFrame(hub_df,geometry=geopandas.points_from_xy(hub_df["hub_lng"],hub_df["hub_lat"]))
#Selecionando os pontos das entregas - mapa entrega
geo_deliveries_df = geopandas.GeoDataFrame(deliveries_df,geometry=geopandas.points_from_xy(deliveries_df["delivery_lng"],deliveries_df["delivery_lat"]))


#Visualização
# cria o plot vazio
fig, ax = plt.subplots(figsize = (50/2.54, 50/2.54))
# plot mapa do distrito federal
mapa.plot(ax=ax, alpha=0.4, color="lightgrey")
# plot das entregas
geo_deliveries_df.query("region == 'df-0'").plot(
 ax=ax, markersize=1, color="red", label="df-0"
)
geo_deliveries_df.query("region == 'df-1'").plot(
 ax=ax, markersize=1, color="blue", label="df-1"
)
geo_deliveries_df.query("region == 'df-2'").plot(
 ax=ax, markersize=1, color="seagreen", label="df-2"
)
# plot dos hubs
geo_hub_df.plot(
 ax=ax, markersize=30, marker="x", color="black", label="hub"
)
# plot da legenda
plt.title(
 "Entregas no Distrito Federal por Região",
 fontdict={"fontsize": 16}
)
lgnd = plt.legend(prop={"size": 15})
for handle in lgnd.legendHandles:
 handle.set_sizes([50])


#2.2. Gráfico de entregas por região

#Selecio nandos os dados
data = pd.DataFrame(deliveries_df[['region', 'vehicle_capacity']].value_counts(normalize=True)).reset_index()
data.rename(columns={0: "region_percent"}, inplace=True)

#Criando e plotando os dados
with sns.axes_style("whitegrid"):
  grafico = sns.barplot(data=data, x="region", y="region_percent",palette="pastel")
  #Editar titulo e legendas
grafico.set(
      title="Percentual de entregas por região",
      xlabel="Região",
      ylabel="Percentual"
  )








