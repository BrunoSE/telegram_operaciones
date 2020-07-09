import psycopg2
import MySQLdb
from random import randint, sample
import threading
import folium
from geopy import distance
import json
import datetime as dt
import pandas as pd
from telegram.ext import Updater, CommandHandler, MessageHandler
import warnings  # para que ubuntu no arroje error
testeo = False  # si se ocupara bot de prueba o no
n_version = "9.3"
ip_webservice = "192.168.11.199"
ip_bd_edu = "192.168.11.150"

warnings.simplefilter(action='ignore', category=FutureWarning)


global lista_acceso_dic
id_bruno_stefoni = "421833900"
infile = open('lista_acceso.json', 'r')
lista_acceso_dic = json.load(infile)
infile.close()

# aca va diccionario con tiempo de viaje por media hora y ruta,
# junto a polilinea de trazado de rutas fts del 104
infile = open('fts104_10metros_modificado_contiempo.json', 'r')
ruta_fts104_10metros = json.load(infile)
infile.close()

nombre_archivo_anexo3 = 'PO 2020(27Jun al 31Dic) UN7 - Anexo 3.xlsx'
Anexo3 = pd.read_excel(nombre_archivo_anexo3, skiprows=6)

servicios_STP = Anexo3['Código TS'].unique().tolist()
servicios_STP = [x.upper().strip() for x in servicios_STP]  # servicios segun anexo3

delta_hacia_atras = 5  # hasta donde se consideran las ultimas transmisiones
criterio_spam = dt.timedelta(seconds=30)  # segundos para reconsultar la base de datos
# segundos para reconsultar la base de datos funcion rorro
criterio_spam_rorro = dt.timedelta(seconds=120)

# esto sirve para que bot no auto-ejecute ultimo comando de salida al reiniciarlo
seguro_telegramearlyexit = False
LosLeones_centroide = [-33.4207887, -70.6079092]  # punto metro Los Leones
ElPennon_centroide = [-33.578444, -70.551972]  # punto cabezal El Penon

# servicios validos como fuera de ruta o descolgados
servicios_validos_FTS = ['F90', 'F91', 'F92', 'F93', 'F94', 'F95', 'FTS']
# patentes L en los tilos y patentes de buses electricos
df_ppu_lostilos = pd.read_excel('ppu_los_tilos.xlsx')
patentesL_lostilos = df_ppu_lostilos.PPU.to_numpy().tolist()
patentesL_electricos = ['LCTG-23', 'LCTG-24', 'LCTG-25', 'LCTG-26', 'LCTG-27', 'LCTG-28',
                        'LCTG-29', 'LCTG-30', 'LCTG-31', 'LCTG-32', 'LCTG-33', 'LCTG-34',
                        'LCTG-35', 'LCTG-36', 'LCTG-37', 'LCTG-38', 'LCTG-39', 'LCTG-40',
                        'LCTG-41', 'LCTG-42', 'LCTG-43', 'LCTG-44', 'LCTG-45', 'LCTG-46',
                        'LCTG-47']

# primeras nueve son para funciones originales f94_104 busesll busesep,
# sirven para reenviar mismo mensaje si se ha consultado en menos de criterio_spam segundos
global ahora_ultima_queryf94_104
global ahora_ultima_querybusesll
global ahora_ultima_querybusesep

global mensaje_ultima_queryf94_104
global mensaje_ultima_querybusesll
global mensaje_ultima_querybusesep

global primera_queryf94_104
global primera_querybusesll
global primera_querybusesep

# estas 3 hacen lo mismo que las 9 anteriores pero son un arreglo para que cada vez que se haga
# una nueva funcion solo haya que aumentar el tamano del arreglo
global ahora_ultima_query_arreglo
global mensaje_ultima_query_arreglo
global primera_query_arreglo

# aumentar este numero a medida que se agregan nuevas funciones
cantidad_de_funciones_query = 6

ahora_ultima_query_arreglo = []
mensaje_ultima_query_arreglo = []
primera_query_arreglo = []

for i in range(cantidad_de_funciones_query):
    ahora_ultima_query_arreglo.append(dt.datetime.now().replace(microsecond=0))
    mensaje_ultima_query_arreglo.append(" ")
    primera_query_arreglo.append(True)


ahora_ultima_queryf94_104 = dt.datetime.now().replace(microsecond=0)
ahora_ultima_querybusesll = dt.datetime.now().replace(microsecond=0)
ahora_ultima_querybusesep = dt.datetime.now().replace(microsecond=0)

mensaje_ultima_queryf94_104 = "."
mensaje_ultima_querybusesll = ".."
mensaje_ultima_querybusesep = "..."

primera_queryf94_104 = True
primera_querybusesll = True
primera_querybusesep = True


# Conexion Servidor Chico para obtener patentes L registradas
db0 = MySQLdb.connect(host=ip_bd_edu,
                      user="brunom",
                      passwd="Manzana",
                      db="stp_portal")

cur0 = db0.cursor()
cur0.execute("SELECT ppu,plaza FROM buses WHERE ppu LIKE 'L%';")
query_ppu_cur0 = cur0.fetchall()
patentesL = [row[0][:4] + "-" + row[0][-2:] for row in query_ppu_cur0]
diccionario_plaza_ppu = {}

for qpp in query_ppu_cur0:
    diccionario_plaza_ppu[qpp[0]] = qpp[1]
    '''
    try:
        diccionario_plaza_ppu[qpp[0]] = int(qpp[1]) # si se quiere int
    except ValueError:
        diccionario_plaza_ppu[qpp[0]] = 0
    '''

cur0.close()
db0.close()

if testeo:
    # llave de bot de prueba
    TOKEN = "602717963:AAGHOVsQRdP_ny142v0YjQfvJ1VD81ZLi1A"
else:
    # llave de bot de operaciones
    TOKEN = "874094838:AAGBvT87g32-4-UnqRGYU2zVVxIu58BjZvs"

updater = Updater(token=TOKEN, use_context=False)


# funcion que obtiene tiempo restante en funcion de la distancia restante
def sacar_tiempo_restante_macul(tiempo_mediahora, d, indice_min):
    if indice_min <= 176:
        return (((d[indice_min] - d[176]) / (d[0] - d[176])) * tiempo_mediahora[0] +
                tiempo_mediahora[1] + tiempo_mediahora[2])
    elif indice_min <= 614:
        return (((d[indice_min] - d[614]) / (d[176] - d[614])) * tiempo_mediahora[1] +
                tiempo_mediahora[2])
    else:
        return tiempo_mediahora[2] * d[indice_min] / d[614]


# funcion de tiempo restante ruta vespucio
def sacar_tiempo_restante_vespucio(tiempo_mediahora, d, indice_min):
    return tiempo_mediahora[3] * d[indice_min] / d[0]


# si punto esta al sur de metro macul
def sur_de_metro_macul(p):
    if p[0] >= -33.580832 and p[0] <= -33.512 and p[1] >= -70.618701 and p[1] <= -70.5471009:
        return True
    else:
        return False


# busca el punto de la polilinea mas cercano al pulso (a menos de 200 metros)
def ajustar_pulsos_a_ruta2(pulso, ruta_macul, ruta_vesp):
    # devuelve arreglo con numero indicando parte de la ruta en que va el bus,
    # indice de polilinea que mejor ajusta pulso a ruta FTS
    parar_busqueda = False
    distancias_pulso_ruta_macul = []

    if sur_de_metro_macul(pulso):
        contador_i = 0
        for t in ruta_macul:
            contador_i = contador_i + 1
            distancia_auxiliar = distance.distance(pulso, t).km
            distancias_pulso_ruta_macul.append(distancia_auxiliar)
            if distancia_auxiliar < 0.020:
                break
            if contador_i > 581:
                break

        indice_minimo_macul = min(range(len(distancias_pulso_ruta_macul)),
                                  key=distancias_pulso_ruta_macul.__getitem__)
        if indice_minimo_macul < 152:
            # devolver 11 si es en camilo henriquez (al sur de Diego Portales)
            # devolver 12 si es en la florida
            return [11, indice_minimo_macul]
        else:
            return [12, indice_minimo_macul]

    contador_i = 0
    distancia_auxiliar = 1
    for t in ruta_macul:
        # partir buscando al norte de geocerca definida por sur_de_metro_macul()
        if contador_i < 579:
            contador_i = contador_i + 1
        else:
            distancia_auxiliar = distance.distance(pulso, t).km
            if distancia_auxiliar < 0.020:
                parar_busqueda = True
                distancias_pulso_ruta_macul.append(distancia_auxiliar)
                break

        distancias_pulso_ruta_macul.append(distancia_auxiliar)

    if parar_busqueda:
        # devolver 13 si es en macul (al sur de Simon Bolivar)
        # devolver 14 si es en los leones
        # si busqueda paro antes de terminal, es ultimo indice
        if (len(distancias_pulso_ruta_macul) - 1) < 1122:
            return [13, (len(distancias_pulso_ruta_macul) - 1)]
        else:
            return [14, (len(distancias_pulso_ruta_macul) - 1)]

    indice_minimo_macul = min(range(len(distancias_pulso_ruta_macul)),
                              key=distancias_pulso_ruta_macul.__getitem__)

    distancias_pulso_ruta_vesp = []
    for v in ruta_vesp:
        distancia_auxiliar = distance.distance(pulso, v).km
        distancias_pulso_ruta_vesp.append(distancia_auxiliar)
        if distancia_auxiliar < 0.020:
            parar_busqueda = True
            break

    if parar_busqueda:
        if (len(distancias_pulso_ruta_vesp) - 1) < 591:
            # devolver 21 si es en vespucio (sur de metro principe de gales)
            return [21, (len(distancias_pulso_ruta_vesp) - 1)]
        else:
            # devolver 22 si es en tobalaba
            return [22, (len(distancias_pulso_ruta_vesp) - 1)]

    else:
        indice_minimo_vesp = min(range(len(distancias_pulso_ruta_vesp)),
                                 key=distancias_pulso_ruta_vesp.__getitem__)
        if ((distancias_pulso_ruta_macul[indice_minimo_macul] <
           distancias_pulso_ruta_vesp[indice_minimo_vesp]) and
           distancias_pulso_ruta_macul[indice_minimo_macul] < 0.200):
            if indice_minimo_macul < 1122:
                return [13, indice_minimo_macul]
            else:
                return [14, indice_minimo_macul]
        elif distancias_pulso_ruta_vesp[indice_minimo_vesp] < 0.200:
            if indice_minimo_vesp < 591:
                return [21, indice_minimo_vesp]
            else:
                return [22, indice_minimo_vesp]
        else:
            return [0, 0]


# devuelve geocerca donde cae,
# la geocerca en ruta es una geocerca gigante que encierra varias comunas
def ubicacion(p):

    if (p[1] >= -33.580831373999935 and p[1] <= -33.576390815942794 and
       p[2] >= -70.55463834229224 and p[2] <= -70.54917176737295):
        return "Cabezal Penon"
    elif (p[1] >= -33.465386618998956 and p[1] <= -33.46095232045423 and
          p[2] >= -70.69815575321851 and p[2] <= -70.6926896346114):
        return "Cabezal 102"
    elif (p[1] >= -33.42285028759169 and p[1] <= -33.41841188936765 and
          p[2] >= -70.61074963488507 and p[2] <= -70.60529056140658):
        return "Cabezal 104"
    elif (p[1] >= -33.46246756 and p[1] <= -33.45802935 and p[2] >= -70.61256359 and
          p[2] <= -70.60710186):
        return "Cabezal 114"
    elif (p[1] >= -33.562534 and p[1] <= -33.560964 and p[2] >= -70.558571 and
          p[1] <= (((-33.562534 - -33.560964) / (-70.555799 - -70.557356)) *
                   (p[2] - -70.557356) + -33.560964)):
        return "Terminal Camilo Henríquez"
    elif p[1] >= -33.580832 and p[1] <= -33.412855 and p[2] >= -70.618701 and p[2] <= -70.5471009:
        return "En ruta"
    else:
        return "NA"


# devuelve la pendiente de una recta
def pendiente(a, b):
    if a[0] - b[0] == 0:
        return 0
    else:
        return (a[1] - b[1]) / (a[0] - b[0])


# devuelve el coeficiente de corte al eje y de una recta dada una pendiente y un punto
def corte(p, g):
    return g[1] - g[0] * p * 1.0


# devuelve los 2 coeficientes de una recta dado dos puntos
def coef_eq_recta(d, e):
    return [pendiente(d, e), corte(pendiente(d, e), d)]


# dados dos puntos calcula la ecuacion de la recta y ordena segun latitud menor y mayor
def eq_geocerca_Romboide(latlon1, latlon2):
    coef = coef_eq_recta((latlon1[1], latlon1[0]), (latlon2[1], latlon2[0]))
    if latlon1[0] < latlon2[0]:
        return [coef[0], coef[1], latlon1[0], latlon2[0]]
    else:
        return [coef[0], coef[1], latlon2[0], latlon1[0]]


coordenadas_geocerca_penon1 = [[-33.579399, -70.551590], [-33.5788656, -70.5519543]]
eq_geocerca_penon1_romboide = eq_geocerca_Romboide(coordenadas_geocerca_penon1[0],
                                                   coordenadas_geocerca_penon1[1])

coordenadas_geocerca_penon2 = [[-33.578866, -70.5519543], [-33.577599, -70.55265]]
eq_geocerca_penon2_romboide = eq_geocerca_Romboide(coordenadas_geocerca_penon2[0],
                                                   coordenadas_geocerca_penon2[1])

coordenadas_geocerca_losleones1 = [[-33.4208332, -70.6084863], [-33.4199595, -70.606523]]
eq_geocerca_losleones1_romboide = eq_geocerca_Romboide(coordenadas_geocerca_losleones1[0],
                                                       coordenadas_geocerca_losleones1[1])


# determina si un punto esta dentro de geocerca penon
def geocerca_ElPenonDetenido_romboideX(p):
    if ((p[0] - eq_geocerca_penon1_romboide[0] * p[1] - eq_geocerca_penon1_romboide[1] > 0) &
       (p[0] - eq_geocerca_penon1_romboide[0] * p[1] - eq_geocerca_penon1_romboide[1] <
       -1.0 * eq_geocerca_penon1_romboide[0] * 0.15) & (p[0] > eq_geocerca_penon1_romboide[2]) &
       (p[0] < eq_geocerca_penon1_romboide[3])):
        return True
    elif ((p[0] - eq_geocerca_penon2_romboide[0] * p[1] - eq_geocerca_penon2_romboide[1] > 0) &
          (p[0] - eq_geocerca_penon2_romboide[0] * p[1] - eq_geocerca_penon2_romboide[1] <
          -1.0 * eq_geocerca_penon2_romboide[0] * 0.15) & (p[0] > eq_geocerca_penon2_romboide[2]) &
          (p[0] < eq_geocerca_penon2_romboide[3])):
        return True
    else:
        return False


# determina si un punto esta dentro de geocerca los leones
def geocerca_LosLeonesDetenido_romboideX(p):
    if ((p[0] - eq_geocerca_losleones1_romboide[0] * p[1] -
        eq_geocerca_losleones1_romboide[1] < 0) &
       (p[0] - eq_geocerca_losleones1_romboide[0] * p[1] - eq_geocerca_losleones1_romboide[1] >
       -1.0 * eq_geocerca_losleones1_romboide[0] * 0.15) &
       (p[0] > eq_geocerca_losleones1_romboide[2]) &
       (p[0] < eq_geocerca_losleones1_romboide[3])):
        return True
    else:
        return False

# devuelve mensaje de tiempo estimado si se pudo estimar


def mensaje_tiempo_estimado(tiempo_estimado):
    if tiempo_estimado > 997:
        return "FR"  # esta fuera de ruta

    return str(int((int(tiempo_estimado) + 1)))

# dibuja mapa de fts


def dibujar_FTS(df_aux):
    m = folium.Map(location=[-33.5, -70.6], tiles='openstreetmap',
                   zoom_start=12, control_scale=True)
    grp_pulsos = folium.FeatureGroup(name='Ultimos Pulsos GPS')
    for i in df_aux.index:
        popup_aux = (df_aux.loc[i, 'PPU'] + " | " + diccionario_plaza_ppu[df_aux.loc[i, 'PPU']] +
                     " | " + str(df_aux.loc[i, 'SSAB']) + " | " + df_aux.loc[i, 'SS_planillon'] +
                     " | " + df_aux.loc[i, 'Ruta_Estimada'] + " | " +
                     mensaje_tiempo_estimado(df_aux.loc[i, 'Tiempo_Viaje_Estimado']))
        folium.Marker([df_aux.loc[i, 'Lat'], df_aux.loc[i, 'Lon']],
                      popup=popup_aux).add_to(grp_pulsos)

    grp_pulsos.add_to(m)
    m.save('Ultimos_pulsosFTS104.html')

# dibuja mapa en cabezales


def dibujar_cabezal(df_aux, nombre_cabezal):

    def coordenadas_poligono_romboide_ejeX(latlon1, latlon2,
                                           pendiente_romboide, Ancho_geocerca_ejeX):
        return [latlon1, latlon2, [latlon2[0], latlon2[1] + Ancho_geocerca_ejeX],
                [latlon1[0], latlon1[1] + Ancho_geocerca_ejeX]]

    centrar_en = [-33.5, -70.575]
    if nombre_cabezal == "ElPenon":
        centrar_en = ElPennon_centroide
    elif nombre_cabezal == "LosLeones":
        centrar_en = LosLeones_centroide

    m = folium.Map(location=centrar_en, tiles='openstreetmap', zoom_start=18, control_scale=True)
    grp_pulsos = folium.FeatureGroup(name='Ultimos Pulsos GPS')
    for i in df_aux.index:

        popup_aux = (df_aux.loc[i, 'PPU'] + " | " + df_aux.loc[i, 'hora'].strftime('%H:%M:%S') +
                     " | " + diccionario_plaza_ppu[df_aux.loc[i, 'PPU']] + " | " +
                     df_aux.loc[i, 'Detenido'])
        folium.Marker([df_aux.loc[i, 'Lat'], df_aux.loc[i, 'Lon']],
                      popup=popup_aux).add_to(grp_pulsos)

    if nombre_cabezal == "LosLeones":

        georcerca_losleones1 = coordenadas_poligono_romboide_ejeX(
            coordenadas_geocerca_losleones1[0], coordenadas_geocerca_losleones1[1],
            eq_geocerca_losleones1_romboide[0], 0.004 * 0.15)
        folium.Polygon(georcerca_losleones1, fill=False, color="purple",
                       popup="LosLeonesDetenido").add_to(m)

    elif nombre_cabezal == "ElPenon":

        georcerca_penon1 = coordenadas_poligono_romboide_ejeX(
            coordenadas_geocerca_penon1[0], coordenadas_geocerca_penon1[1],
            eq_geocerca_penon1_romboide[0], 0.004 * 0.15)
        folium.Polygon(georcerca_penon1, fill=False, color="purple",
                       popup="PenonDetenido1").add_to(m)
        georcerca_penon2 = coordenadas_poligono_romboide_ejeX(
            coordenadas_geocerca_penon2[0], coordenadas_geocerca_penon2[1],
            eq_geocerca_penon2_romboide[0], 0.004 * 0.15)
        folium.Polygon(georcerca_penon2, fill=False, color="purple",
                       popup="PenonDetenido2").add_to(m)

    grp_pulsos.add_to(m)
    m.save('Buses' + nombre_cabezal + '.html')


def consultar_fts_104():

    global ahora_ultima_queryf94_104
    global mensaje_ultima_queryf94_104
    global primera_queryf94_104

    ahora = dt.datetime.now().replace(microsecond=0)
    delta_query = dt.timedelta(seconds=1)

    if ahora > ahora_ultima_queryf94_104:
        delta_query = ahora - ahora_ultima_queryf94_104

    if delta_query > criterio_spam or primera_queryf94_104:

        primera_queryf94_104 = False
        ahora_ultima_queryf94_104 = ahora
        ahora_delta = ahora - dt.timedelta(minutes=delta_hacia_atras)

        #   SERVIDOR CHICO
        db1 = MySQLdb.connect(host=ip_bd_edu,
                              user="brunom",
                              passwd="Manzana",
                              db="repositorios")

        cur1 = db1.cursor()

        cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido," +
                     "fecha, hora, servicio_sentido_a_bordo_del_bus, estado, " +
                     "velocidad_instantanea_del_bus, tiempo_detenido FROM ultimas_transmisiones;")
        datos = [row for row in cur1.fetchall() if row[0] in patentesL and row[1] is not None and
                 row[7][:3] in servicios_validos_FTS and (not row[0] in patentesL_lostilos) and
                 (not row[0] in patentesL_electricos)]
        datosOK = [[row[0], float(row[1]), float(row[2]), row[3], row[4], row[5], row[6],
                   row[7], row[8], int(row[9]), int(row[10])] for row in datos]
        datos_FTS = [row for row in datosOK if dt.datetime.combine(
                     row[5], (dt.datetime.min + row[6]).time()) > ahora_delta and
                     ubicacion(row) == 'En ruta']

        df = pd.DataFrame(datos_FTS, columns=['PPU', 'Lat', 'Lon', 'Sentido',
                                              'Servicio', 'fecha', 'hora', 'SSAB', 'Estado',
                                              'V_inst', 'T_detenido'])

        if not df.empty:
            df['Sentido_SSAB'] = df.SSAB.str[-1]
            df = df.loc[df['Sentido_SSAB'] == 'R']

        if df.empty:
            mensaje_telegram = "No se encontraron F94 en ruta"

        else:
            #   SERVIDOR CHICO
            db2 = MySQLdb.connect(host=ip_bd_edu,
                                  user="brunom",
                                  passwd="Manzana",
                                  db="stp_portal")

            cur2 = db2.cursor()

            df['PPU'] = df.PPU.str.replace('-', '')
            df.drop_duplicates(subset=['PPU'], inplace=True)
            df['hora'] = pd.to_datetime(df['hora'])
            df['Media_hra'] = df.apply(lambda x:
                                       x['hora'] - pd.Timedelta(minutes=(x['hora'].minute % 30),
                                                                seconds=x['hora'].second),
                                       axis=1).dt.strftime("%H:%M")

            # df['dX'] = 999
            # df.loc[df['Sentido_SSAB']=='R','dX'] = df.loc[df['Sentido_SSAB']=='R'].apply(
            # lambda x: distance.distance((x['Lat'], x['Lon']),
            # (LosLeones_centroide[0], LosLeones_centroide[1])).km, axis = 1)
            # df.loc[df['Sentido_SSAB']=='I','dX'] = df.loc[df['Sentido_SSAB']=='I'].apply(
            # lambda x: distance.distance((x['Lat'], x['Lon']),
            # (ElPennon_centroide[0], ElPennon_centroide[1])).km, axis = 1)
            # df['Tiempo_Viaje_Estimado'] = df['dX']/0.334

            df['Tiempo_Viaje_Estimado'] = 998
            df['Ruta_Estimada'] = "FR"

            ppus_encontradas = str(df.PPU.values.tolist())
            ppus_encontradas = '(' + ppus_encontradas[1:-1] + ')'

            timestamp_limite = (dt.datetime.today() - dt.timedelta(minutes=120))
            fecha_limite = timestamp_limite.strftime('%Y-%m-%d')
            hora_limite = timestamp_limite.strftime('%H:%M:%S')
            query2 = ("SELECT ppu, fecha, horaprogramada, horasalidareal, servicio, sentido, " +
                      "conductor_nombre FROM planillon_despachos WHERE fts = 'S' AND fecha >= '" +
                      fecha_limite + "' AND horaprogramada >= '" + hora_limite + "' AND ppu in " +
                      ppus_encontradas + ";")
            cur2.execute(query2)
            datos_planillon = [row for row in cur2.fetchall()]

            df_planillon = pd.DataFrame(datos_planillon, columns=[
                                        'PPU', 'Fecha_planillon', 'Hora_Programada_planillon',
                                        'Hora_Salida_Real_planillon', 'Servicio_planillon',
                                        'Sentido_planillon', 'Nombre_Conductor'])
            df_planillon['SS_planillon'] = (df_planillon['Servicio_planillon'] +
                                            df_planillon['Sentido_planillon'])

            df = df.merge(df_planillon, how='left', left_on=['PPU'], right_on=['PPU'])
            df = df.loc[(df['SS_planillon'].str[:-1] == 'F74') | (df['SS_planillon'].isnull())]

            for i in df.index:

                ajuste_a_pulso = ajustar_pulsos_a_ruta2(
                    [df.loc[i, 'Lat'], df.loc[i, 'Lon']],
                    ruta_fts104_10metros['104_FTS_R_Macul']['polilinea'],
                    ruta_fts104_10metros['104_FTS_R_Vesp']['polilinea'])

                if ajuste_a_pulso[0] >= 11 and ajuste_a_pulso[0] <= 19:  # usar polilinea macul
                    df.loc[i, 'Tiempo_Viaje_Estimado'] = sacar_tiempo_restante_macul(
                        ruta_fts104_10metros['Tiempos_de_viaje'][df.loc[i, 'Media_hra']],
                        ruta_fts104_10metros['104_FTS_R_Macul']['dist_acum'], ajuste_a_pulso[1])
                    if ajuste_a_pulso[0] == 11:
                        df.loc[i, 'Ruta_Estimada'] = "CamilH"
                    elif ajuste_a_pulso[0] == 12:
                        df.loc[i, 'Ruta_Estimada'] = "Florida"
                    elif ajuste_a_pulso[0] == 13:
                        df.loc[i, 'Ruta_Estimada'] = "Macul  "
                    elif ajuste_a_pulso[0] == 14:
                        df.loc[i, 'Ruta_Estimada'] = "LLeones"

                elif ajuste_a_pulso[0] >= 21 and ajuste_a_pulso[0] <= 29:  # usar polilinea vespucio
                    df.loc[i, 'Tiempo_Viaje_Estimado'] = sacar_tiempo_restante_vespucio(
                        ruta_fts104_10metros['Tiempos_de_viaje'][df.loc[i, 'Media_hra']],
                        ruta_fts104_10metros['104_FTS_R_Vesp']['dist_acum'], ajuste_a_pulso[1])
                    if ajuste_a_pulso[0] == 21:
                        df.loc[i, 'Ruta_Estimada'] = "Vespuc"
                    elif ajuste_a_pulso[0] == 22:
                        df.loc[i, 'Ruta_Estimada'] = "Tobalb"

                else:
                    df.loc[i, 'Tiempo_Viaje_Estimado'] = 999

            df = df.loc[~((df['Ruta_Estimada'] == "FR") & (df['SS_planillon'].isnull()))]

            df.sort_values(by=['Tiempo_Viaje_Estimado'], inplace=True)

            numero_fts_f74_planillon = len(df.loc[df['SS_planillon'].str[:-1] == 'F74'].index)
            df.loc[df['SS_planillon'].isnull(), 'SS_planillon'] = '  ?   '

            if len(df.index) == 1:
                mensaje_telegram = "Hay 1 bus F94 hacia Los Leones, según planillon online:\n"
            else:
                mensaje_telegram = ("Hay " + str(len(df.index)) +
                                    " buses F94 hacia Los Leones, según planillon online " +
                                    str(numero_fts_f74_planillon) + " son para 104:\n")

            mensaje_telegram = mensaje_telegram + "Patente | Plazas | Serv | Ruta | Minutos\n"

            for i in df.index:
                # df.loc[i,'SSAB'] casi siempre tira F94 05R
                mensaje_telegram = (mensaje_telegram + df.loc[i, 'PPU'] + "  |    " +
                                    diccionario_plaza_ppu[df.loc[i, 'PPU']] + "    | " +
                                    df.loc[i, 'SS_planillon'] + " | " +
                                    df.loc[i, 'Ruta_Estimada'] + " |  " +
                                    mensaje_tiempo_estimado(df.loc[i, 'Tiempo_Viaje_Estimado']) +
                                    "\n")

            cur2.close()
            db2.close()

        cur1.close()
        db1.close()
        # dibujar_FTS(df)

        mensaje_ultima_queryf94_104 = mensaje_telegram
        return mensaje_ultima_queryf94_104

    else:
        return "-" + mensaje_ultima_queryf94_104


def consultar_buses_cabezal_LosLeones():
    global ahora_ultima_querybusesll
    global mensaje_ultima_querybusesll
    global primera_querybusesll

    ahora = dt.datetime.now().replace(microsecond=0)
    delta_query = dt.timedelta(seconds=1)

    if ahora > ahora_ultima_querybusesll:
        delta_query = ahora - ahora_ultima_querybusesll

    if delta_query > criterio_spam or primera_querybusesll:

        primera_querybusesll = False
        ahora_ultima_querybusesll = ahora
        ahora_delta = ahora - dt.timedelta(minutes=delta_hacia_atras)

        #   SERVIDOR CHICO
        db1 = MySQLdb.connect(host=ip_bd_edu,
                              user="brunom",
                              passwd="Manzana",
                              db="repositorios")

        cur1 = db1.cursor()

        cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido, fecha, hora, " +
                     "servicio_sentido_a_bordo_del_bus, estado, velocidad_instantanea_del_bus, " +
                     "tiempo_detenido FROM ultimas_transmisiones;")
        datos = [row for row in cur1.fetchall() if row[0] in patentesL and
                 row[1] is not None and (not row[0] in patentesL_lostilos) and
                 (not row[0] in patentesL_electricos)]
        datosOK = [[row[0], float(row[1]), float(row[2]), row[3], row[4], row[5], row[6],
                    row[7], row[8], int(row[9]), int(row[10])] for row in datos]
        datos_cabezal = [row for row in datosOK if dt.datetime.combine(
                         row[5], (dt.datetime.min + row[6]).time()) > ahora_delta and
                         ubicacion(row) == "Cabezal 104"]
        datos_cabezal = [[row[0], row[1], row[2], row[6], row[8], int(row[10])] for
                         row in datos_cabezal]

        df = pd.DataFrame(datos_cabezal, columns=['PPU', 'Lat', 'Lon', 'hora',
                                                  'Estado', 'T_detenido'])

        if df.empty:
            mensaje_telegram = "No se encontraron buses en cabezal Los Leones"

        else:
            df['PPU'] = df.PPU.str.replace('-', '')
            df.drop_duplicates(subset=['PPU'], inplace=True)
            df['hora'] = pd.to_datetime(df['hora'])

            df['Detenido'] = 'NO'
            for i in df.index:
                if geocerca_LosLeonesDetenido_romboideX([df.loc[i, 'Lat'], df.loc[i, 'Lon']]):
                    df.loc[i, 'Detenido'] = 'SI'

            df.sort_values(by=['Detenido'], inplace=True, ascending=False)
            df2 = df.loc[df['Detenido'] == 'SI']
            if len(df2.index) == 1:
                mensaje_telegram = "Hay 1 bus detenido en cabezal Los Leones\n"
            else:
                mensaje_telegram = ("Hay " + str(len(df.index)) +
                                    " buses cerca de cabezal Los Leones, " +
                                    str(len(df2.index)) + " detenidos\n")

            mensaje_telegram = mensaje_telegram + "Patente | Hora GPS | Plazas | Detenido\n"

            for i in df.index:
                mensaje_telegram = (mensaje_telegram + df.loc[i, 'PPU'] + "  |    " +
                                    df.loc[i, 'hora'].strftime('%H:%M:%S') + "    |   " +
                                    diccionario_plaza_ppu[df.loc[i, 'PPU']] + "  | " +
                                    df.loc[i, 'Detenido'] + "\n")

        cur1.close()
        db1.close()

        # dibujar_cabezal(df,'LosLeones')
        mensaje_ultima_querybusesll = mensaje_telegram
        return mensaje_ultima_querybusesll

    else:
        return "-" + mensaje_ultima_querybusesll


def consultar_buses_cabezal_ElPenon():
    global ahora_ultima_querybusesep
    global mensaje_ultima_querybusesep
    global primera_querybusesep

    ahora = dt.datetime.now().replace(microsecond=0)
    delta_query = dt.timedelta(seconds=1)

    if ahora > ahora_ultima_querybusesep:
        delta_query = ahora - ahora_ultima_querybusesep

    if delta_query > criterio_spam or primera_querybusesep:

        primera_querybusesep = False
        ahora_ultima_querybusesep = ahora
        ahora_delta = ahora - dt.timedelta(minutes=delta_hacia_atras)

        db1 = MySQLdb.connect(host=ip_bd_edu,
                              user="brunom",
                              passwd="Manzana",
                              db="repositorios")

        cur1 = db1.cursor()

        cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido, fecha, hora, " +
                     "servicio_sentido_a_bordo_del_bus, estado, velocidad_instantanea_del_bus, " +
                     "tiempo_detenido FROM ultimas_transmisiones;")
        datos = [row for row in cur1.fetchall() if row[0] in patentesL and
                 row[1] is not None and (not row[0] in patentesL_lostilos) and
                 (not row[0] in patentesL_electricos)]
        datosOK = [[row[0], float(row[1]), float(row[2]), row[3], row[4], row[5], row[6],
                    row[7], row[8], int(row[9]), int(row[10])] for row in datos]
        datos_cabezal = [row for row in datosOK if dt.datetime.combine(
                         row[5], (dt.datetime.min + row[6]).time()) > ahora_delta and
                         ubicacion(row) == "Cabezal Penon"]
        datos_cabezal = [[row[0], row[1], row[2], row[6], row[8], int(row[10])] for
                         row in datos_cabezal]

        df = pd.DataFrame(datos_cabezal, columns=['PPU', 'Lat', 'Lon', 'hora',
                                                  'Estado', 'T_detenido'])

        if df.empty:
            mensaje_telegram = "No se encontraron buses en cabezal El Penon"

        else:
            df['PPU'] = df.PPU.str.replace('-', '')
            df.drop_duplicates(subset=['PPU'], inplace=True)
            df['hora'] = pd.to_datetime(df['hora'])

            df['Detenido'] = 'NO'
            for i in df.index:
                if geocerca_ElPenonDetenido_romboideX([df.loc[i, 'Lat'], df.loc[i, 'Lon']]):
                    df.loc[i, 'Detenido'] = 'SI'

            df.sort_values(by=['Detenido'], inplace=True, ascending=False)
            df2 = df.loc[df['Detenido'] == 'SI']
            if len(df2.index) == 1:
                mensaje_telegram = "Hay 1 bus detenido en cabezal El Penon\n"
            else:
                mensaje_telegram = ("Hay " + str(len(df.index)) +
                                    " buses cerca de cabezal El Penon, " +
                                    str(len(df2.index)) + " detenidos\n")

            mensaje_telegram = mensaje_telegram + "Patente | Hora GPS | Plazas | Detenido\n"

            for i in df.index:
                mensaje_telegram = (mensaje_telegram + df.loc[i, 'PPU'] + "  |    " +
                                    df.loc[i, 'hora'].strftime('%H:%M:%S') + "    |   " +
                                    diccionario_plaza_ppu[df.loc[i, 'PPU']] + "  | " +
                                    df.loc[i, 'Detenido'] + "\n")

        cur1.close()
        db1.close()

        # dibujar_cabezal(df,'ElPenon')
        mensaje_ultima_querybusesep = mensaje_telegram
        return mensaje_ultima_querybusesep

    else:
        return "-" + mensaje_ultima_querybusesep


def consultar_rorro():
    numero_de_funcion_query = 0
    global ahora_ultima_query_arreglo
    global mensaje_ultima_query_arreglo
    global primera_query_arreglo

    ahora = dt.datetime.now().replace(microsecond=0)
    delta_query = dt.timedelta(seconds=1)

    if ahora > ahora_ultima_query_arreglo[numero_de_funcion_query]:
        delta_query = ahora - ahora_ultima_query_arreglo[numero_de_funcion_query]

    if delta_query > criterio_spam_rorro or primera_query_arreglo[numero_de_funcion_query]:
        df = pd.DataFrame(columns=['a'])

        primera_query_arreglo[numero_de_funcion_query] = False
        ahora_ultima_query_arreglo[numero_de_funcion_query] = ahora
        ahora_delta = ahora - dt.timedelta(minutes=delta_hacia_atras)

        db1 = MySQLdb.connect(host=ip_bd_edu,
                              user="brunom",
                              passwd="Manzana",
                              db="repositorios")

        cur1 = db1.cursor()
        cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido, fecha, hora, " +
                     "servicio_sentido_a_bordo_del_bus, estado, velocidad_instantanea_del_bus, " +
                     "tiempo_detenido, idwebservice FROM ultimas_transmisiones;")

        datos = [row for row in cur1.fetchall() if len(row) == 12 and row[1] is not None]
        print("Hay %d datos en las ultimas transmisiones" % len(datos))
        datosOK = [[row[0], float(row[1]), float(row[2]), row[3], row[4], row[5], row[6],
                    row[7], row[8], int(row[9]), int(row[10]), str(row[11])] for row in datos]

        datos_cabezal = [row for row in datosOK if dt.datetime.combine(
            row[5], (dt.datetime.min + row[6]).time()) > ahora_delta]
        datos_cabezal = [[row[0], row[1], row[2], row[6], row[8], row[10], row[11]] for
                         row in datos_cabezal]

        if datos_cabezal:
            datos_cabezal = [datos_cabezal[randint(0, (len(datos_cabezal) - 1))]]
            df = pd.DataFrame(datos_cabezal, columns=['PPU', 'Lat', 'Lon', 'hora',
                                                      'Estado', 'T_detenido', 'idwebservice'])

        if df.empty:
            mensaje_telegram = "No se encontraron buses"

        elif len(df.index) == 1:

            df['PPU'] = df.PPU.str.replace('-', '')
            df.drop_duplicates(subset=['PPU'], inplace=True)
            df['hora'] = pd.to_datetime(df['hora'])

            df['estado_ignicion'] = "sin ignición"
            i = df.index[0]

            db3 = psycopg2.connect(host=ip_webservice,
                                   user="webservice",
                                   password="Sasser.a36*",
                                   database="webservice")

            cur3 = db3.cursor()
            cur3.execute("SELECT estado_ignicion FROM ws_pos_dia_2019 WHERE id = " +
                         df.loc[i, 'idwebservice'])

            df.loc[i, 'estado_ignicion'] = cur3.fetchall()[0][0]

            mensaje_telegram = "Patente | Estado Ignicion | Hora\n"
            mensaje_telegram = (mensaje_telegram + df.loc[i, 'PPU'] + "  |  " +
                                df.loc[i, 'estado_ignicion'] + "   | " +
                                df.loc[i, 'hora'].strftime('%H:%M:%S') + "\n")

            cur3.close()
            db3.close()

        else:
            mensaje_telegram = "Si, pero no"

        cur1.close()
        db1.close()
        mensaje_ultima_query_arreglo[numero_de_funcion_query] = mensaje_telegram
        return mensaje_ultima_query_arreglo[numero_de_funcion_query]

    else:
        return "-" + mensaje_ultima_query_arreglo[numero_de_funcion_query]


def consultar_ultima_transmision_10():
    tamanno_muestra = 10
    numero_de_funcion_query = 1
    global ahora_ultima_query_arreglo
    global mensaje_ultima_query_arreglo
    global primera_query_arreglo

    ahora = dt.datetime.now().replace(microsecond=0)
    delta_query = dt.timedelta(seconds=1)

    if ahora > ahora_ultima_query_arreglo[numero_de_funcion_query]:
        delta_query = ahora - ahora_ultima_query_arreglo[numero_de_funcion_query]

    if delta_query > criterio_spam or primera_query_arreglo[numero_de_funcion_query]:

        primera_query_arreglo[numero_de_funcion_query] = False
        ahora_ultima_query_arreglo[numero_de_funcion_query] = ahora

        db1 = MySQLdb.connect(host=ip_bd_edu,
                              user="brunom",
                              passwd="Manzana",
                              db="repositorios")

        cur1 = db1.cursor()

        cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido, fecha, hora, " +
                     "servicio_sentido_a_bordo_del_bus, estado, velocidad_instantanea_del_bus, " +
                     "tiempo_detenido, idwebservice, ubicacion FROM ultimas_transmisiones;")

        datos = [row for row in cur1.fetchall()]
        datos = [datos[i] for i in sample(range(0, (len(datos) - 1)), tamanno_muestra)]
        datosOK = [[row[0], float(row[1]), float(row[2]), row[3], row[4], row[5], row[6], row[7],
                   row[8], int(row[9]), int(row[10]), str(row[11]), str(row[12])] for row in datos]
        datos_cabezal = [[row[0], row[1], row[2], row[6], row[8], row[10], row[11], row[12]] for
                         row in datosOK]

        df = pd.DataFrame(datos_cabezal, columns=['PPU', 'Lat', 'Lon', 'hora', 'Estado',
                                                  'T_detenido', 'idwebservice', 'ubicacion'])

        if df.empty:
            mensaje_telegram = "No se encontraron buses"

        else:
            df['PPU'] = df.PPU.str.replace('-', '')
            df.drop_duplicates(subset=['PPU'], inplace=True)
            df['hora'] = pd.to_datetime(df['hora'])

            mensaje_telegram = ("Se sacó una muestra de " + str(len(df.index)) +
                                " buses entre las últimas transmisiones\n")
            mensaje_telegram = mensaje_telegram + "Patente |   Hora   | Ubicación\n"

            for i in df.index:
                mensaje_telegram = (mensaje_telegram + df.loc[i, 'PPU'] + " | " +
                                    df.loc[i, 'hora'].strftime('%H:%M:%S') + " | " +
                                    df.loc[i, 'ubicacion'] + "\n")

        cur1.close()
        db1.close()

        mensaje_ultima_query_arreglo[numero_de_funcion_query] = mensaje_telegram
        return mensaje_ultima_query_arreglo[numero_de_funcion_query]

    else:
        return "-" + mensaje_ultima_query_arreglo[numero_de_funcion_query]


def consultar_ultima_transmision_electricos():
    numero_de_funcion_query = 2
    global ahora_ultima_query_arreglo
    global mensaje_ultima_query_arreglo
    global primera_query_arreglo

    ahora = dt.datetime.now().replace(microsecond=0)
    delta_query = dt.timedelta(seconds=1)

    if ahora > ahora_ultima_query_arreglo[numero_de_funcion_query]:
        delta_query = ahora - ahora_ultima_query_arreglo[numero_de_funcion_query]

    if delta_query > criterio_spam or primera_query_arreglo[numero_de_funcion_query]:

        primera_query_arreglo[numero_de_funcion_query] = False
        ahora_ultima_query_arreglo[numero_de_funcion_query] = ahora

        db1 = MySQLdb.connect(host=ip_bd_edu,
                              user="brunom",
                              passwd="Manzana",
                              db="repositorios")

        cur1 = db1.cursor()

        cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido, fecha, hora, " +
                     "servicio_sentido_a_bordo_del_bus, estado, velocidad_instantanea_del_bus, " +
                     "tiempo_detenido, idwebservice, ubicacion FROM ultimas_transmisiones;")

        datos = [row for row in cur1.fetchall() if row[0] in patentesL_electricos and
                 row[1] is not None]
        datosOK = [[row[0], float(row[1]), float(row[2]), row[3], row[4], row[5], row[6], row[7],
                   row[8], int(row[9]), int(row[10]), str(row[11]), str(row[12])] for row in datos]
        datos_cabezal = [[row[0], row[1], row[2], row[6], row[7], row[8],
                          row[10], row[11], row[12]] for row in datosOK]

        df = pd.DataFrame(datos_cabezal, columns=['PPU', 'Lat', 'Lon', 'hora',
                                                  'SSAB', 'Estado', 'T_detenido',
                                                  'idwebservice', 'ubicacion'])

        if df.empty:
            mensaje_telegram = "No se encontraron buses"

        else:
            df['Sentido_SSAB'] = df['SSAB'].str[-1]
            df['Serv_SSAB'] = df['SSAB'].str[:3]
            df['SS'] = df['Serv_SSAB'] + df['Sentido_SSAB']

            df['PPU'] = df.PPU.str.replace('-', '')
            df.drop_duplicates(subset=['PPU'], inplace=True)
            df['hora'] = pd.to_datetime(df['hora'])

            df.loc[df['ubicacion'] == 'Fuera Ruta', 'ubicacion'] = 'a Fuera Ruta'
            df.loc[df['ubicacion'] == 'En Ruta sin Tx', 'ubicacion'] = 'b En Ruta sin Tx'
            df.loc[df['ubicacion'] == 'En Transito', 'ubicacion'] = 'c En Transito'

            df.loc[df['ubicacion'] == 'En Terminal', 'ubicacion'] = 'z En Terminal'
            df.loc[df['ubicacion'] == 'Cabezal Inicio Ret', 'ubicacion'] = 'y Cabezal Ret'

            df.loc[df['ubicacion'] == 'Cabezal Inicio Ida', 'ubicacion'] = 'CabezalIda'

            df.sort_values(by=['ubicacion', 'Sentido_SSAB', 'hora'], inplace=True, ascending=False)

            df.loc[df['ubicacion'] == 'y Cabezal Ret', 'ubicacion'] = 'CabezalRet'
            df.loc[df['ubicacion'] == 'z En Terminal', 'ubicacion'] = 'Terminal'

            df.loc[df['ubicacion'] == 'a Fuera Ruta', 'ubicacion'] = 'FueraRuta'
            df.loc[df['ubicacion'] == 'b En Ruta sin Tx', 'ubicacion'] = 'EnRuta sinTx'
            df.loc[df['ubicacion'] == 'c En Transito', 'ubicacion'] = 'EnTransito'

            mensaje_telegram = ("Se encontraron " + str(len(df.index)) +
                                " buses eléctricos en las últimas transmisiones\n")
            mensaje_telegram = mensaje_telegram + "Patente |   Hora   | SSAB | Ubicación \n"

            for i in df.index:
                mensaje_telegram = (mensaje_telegram + df.loc[i, 'PPU'] + " | " +
                                    df.loc[i, 'hora'].strftime('%H:%M:%S') + " | " +
                                    df.loc[i, 'SS'] + " | " + df.loc[i, 'ubicacion'] + "\n")

        cur1.close()
        db1.close()

        mensaje_ultima_query_arreglo[numero_de_funcion_query] = mensaje_telegram
        return mensaje_ultima_query_arreglo[numero_de_funcion_query]

    else:
        return "-" + mensaje_ultima_query_arreglo[numero_de_funcion_query]


def consultar_patentes_ultima_transmision_maipu():
    numero_de_funcion_query = 3
    global ahora_ultima_query_arreglo
    global mensaje_ultima_query_arreglo
    global primera_query_arreglo

    ahora = dt.datetime.now().replace(microsecond=0)
    delta_query = dt.timedelta(seconds=1)

    if ahora > ahora_ultima_query_arreglo[numero_de_funcion_query]:
        delta_query = ahora - ahora_ultima_query_arreglo[numero_de_funcion_query]

    if delta_query > criterio_spam or primera_query_arreglo[numero_de_funcion_query]:

        primera_query_arreglo[numero_de_funcion_query] = False
        ahora_ultima_query_arreglo[numero_de_funcion_query] = ahora

        db1 = MySQLdb.connect(host=ip_bd_edu,
                              user="brunom",
                              passwd="Manzana",
                              db="repositorios")

        cur1 = db1.cursor()

        cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido, fecha, hora, " +
                     "servicio_sentido_a_bordo_del_bus, estado, velocidad_instantanea_del_bus, " +
                     "tiempo_detenido, idwebservice, ubicacion FROM ultimas_transmisiones_c;")

        datos = [row for row in cur1.fetchall()]
        datosOK = [[row[0], float(row[1]), float(row[2]), row[3], row[4], row[5], row[6], row[7],
                   row[8], int(row[9]), int(row[10]), str(row[11]), str(row[12])] for row in datos]
        datos_cabezal = [[row[0], row[1], row[2], row[6], row[8], row[10], row[11], row[12]] for
                         row in datosOK]

        df = pd.DataFrame(datos_cabezal, columns=['PPU', 'Lat', 'Lon', 'hora',
                                                  'Estado', 'T_detenido', 'idwebservice',
                                                  'ubicacion'])

        if df.empty:
            mensaje_telegram = "No se encontraron buses"

        else:
            df['PPU'] = df.PPU.str.replace('-', '')
            df.drop_duplicates(subset=['PPU'], inplace=True)
            df['hora'] = pd.to_datetime(df['hora'])

            mensaje_telegram = ("Hay " + str(len(df.index)) +
                                " patentes distintas entre las últimas transmisiones\n")
            mensaje_telegram = mensaje_telegram + "Iniciales Patente |  Conteo\n"

            df = df['PPU'].str[:2].value_counts()

            for ppu, x in df.iteritems():
                mensaje_telegram = (mensaje_telegram + ppu +
                                    "                           |  " +
                                    str(x) + "\n")
            mensaje_telegram = mensaje_telegram + 'Total                    |  ' + str(df.sum())

        cur1.close()
        db1.close()

        mensaje_ultima_query_arreglo[numero_de_funcion_query] = mensaje_telegram
        return mensaje_ultima_query_arreglo[numero_de_funcion_query]

    else:
        return "-" + mensaje_ultima_query_arreglo[numero_de_funcion_query]


def consultar_numero_ultimas_transmisiones():
    numero_de_funcion_query = 4
    global ahora_ultima_query_arreglo
    global mensaje_ultima_query_arreglo
    global primera_query_arreglo

    ahora = dt.datetime.now().replace(microsecond=0)
    delta_query = dt.timedelta(seconds=1)

    if ahora > ahora_ultima_query_arreglo[numero_de_funcion_query]:
        delta_query = ahora - ahora_ultima_query_arreglo[numero_de_funcion_query]

    if delta_query > criterio_spam_rorro or primera_query_arreglo[numero_de_funcion_query]:
        primera_query_arreglo[numero_de_funcion_query] = False
        ahora_ultima_query_arreglo[numero_de_funcion_query] = ahora

        db1 = MySQLdb.connect(host=ip_bd_edu,
                              user="brunom",
                              passwd="Manzana",
                              db="repositorios")

        cur1 = db1.cursor()
        cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido, fecha, hora, " +
                     "servicio_sentido_a_bordo_del_bus, estado, velocidad_instantanea_del_bus, " +
                     "tiempo_detenido, idwebservice FROM ultimas_transmisiones;")

        datos = [row for row in cur1.fetchall() if len(row) == 12 and row[1] is not None]
        mensaje_telegram = ("Hay " + str(len(datos)) + " registros en la tabla " +
                            "ultimas_transmisiones")
        print(mensaje_telegram)

        mensaje_ultima_query_arreglo[numero_de_funcion_query] = mensaje_telegram
        return mensaje_ultima_query_arreglo[numero_de_funcion_query]

    else:
        return "-" + mensaje_ultima_query_arreglo[numero_de_funcion_query]


def consultar_numero_ultimas_transmisiones_maipu():
    numero_de_funcion_query = 5
    global ahora_ultima_query_arreglo
    global mensaje_ultima_query_arreglo
    global primera_query_arreglo

    ahora = dt.datetime.now().replace(microsecond=0)
    delta_query = dt.timedelta(seconds=1)

    if ahora > ahora_ultima_query_arreglo[numero_de_funcion_query]:
        delta_query = ahora - ahora_ultima_query_arreglo[numero_de_funcion_query]

    if delta_query > criterio_spam_rorro or primera_query_arreglo[numero_de_funcion_query]:
        primera_query_arreglo[numero_de_funcion_query] = False
        ahora_ultima_query_arreglo[numero_de_funcion_query] = ahora

        db1 = MySQLdb.connect(host=ip_bd_edu,
                              user="brunom",
                              passwd="Manzana",
                              db="repositorios")

        cur1 = db1.cursor()
        cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido, fecha, hora, " +
                     "servicio_sentido_a_bordo_del_bus, estado, velocidad_instantanea_del_bus, " +
                     "tiempo_detenido, idwebservice FROM ultimas_transmisiones_c;")

        datos = [row for row in cur1.fetchall() if len(row) == 12 and row[1] is not None]
        mensaje_telegram = ("Hay " + str(len(datos)) + " registros en la tabla " +
                            "ultimas_transmisiones_c (maipu)")
        print(mensaje_telegram)

        mensaje_ultima_query_arreglo[numero_de_funcion_query] = mensaje_telegram
        return mensaje_ultima_query_arreglo[numero_de_funcion_query]

    else:
        return "-" + mensaje_ultima_query_arreglo[numero_de_funcion_query]


def consultar_anexo3(servicio="F01", sentido="Ida", tipo_dia="Laboral", periodo="ts"):
    mensaje_telegram = ""

    if sentido == "i":
        sentido = "Ida"
    elif sentido == "r":
        sentido = "Ret"

    if tipo_dia == "l":
        tipo_dia = "Laboral"
    elif tipo_dia == "s":
        tipo_dia = "Sábado"
    elif tipo_dia == "d":
        tipo_dia = "Domingo"

    if (servicio in servicios_STP and sentido in ['Ida', 'Ret'] and
       tipo_dia in ['Laboral', 'Sábado', 'Domingo'] and periodo in ['mh', 'ts']):
        mensaje_telegram = "Anexo 3 " + servicio + sentido[0] + " " + tipo_dia[:3] + "\n"
        query_anexo3 = Anexo3.loc[(Anexo3['Código TS'] == servicio) & (
            Anexo3['Sentido'] == sentido) & (Anexo3['Día'] == tipo_dia)]
        if periodo == 'mh':
            mensaje_telegram = mensaje_telegram + "MH | Salidas | Cap. Plazas\n"
            for q in query_anexo3.index:
                mensaje_telegram = (mensaje_telegram + query_anexo3.loc[q, 'MH'].strftime('%H:%M') +
                                    " |   " + str(query_anexo3.loc[q, 'N° Salidas']) + "     |  " +
                                    str(query_anexo3.loc[q, 'Capacidad (plazas)']) + "\n")
        else:
            mensaje_telegram = mensaje_telegram + "PeriodoTS | Salidas\n"
            query_anexo3_procesada = query_anexo3.groupby(['Periodo'])[
                'Velocidad (Km/hra)', 'Distancia Base (Km)', 'Distancia Total (POB+POI) (Km)',
                'Tiempo recorrido (hra)', 'Capacidad (plazas)'].transform('mean')
            query_anexo3_procesada['Salidas_periodo'] = query_anexo3.groupby(['Periodo'])[
                'N° Salidas'].transform('sum')
            query_anexo3_procesada['Periodo'] = query_anexo3['Periodo']
            query_anexo3_procesada.drop_duplicates(subset=['Periodo'], inplace=True)
            for q in query_anexo3_procesada.index:
                string_columna = "   "
                # string_columna2 = "    |  "
                if str(query_anexo3_procesada.loc[q, 'Periodo'])[:2] in ['02', '04', '07', '10']:
                    string_columna = "          " + string_columna
                elif str(query_anexo3_procesada.loc[q, 'Periodo'])[:2] in ['03', '05', '06']:
                    string_columna = "        " + string_columna
                elif str(query_anexo3_procesada.loc[q, 'Periodo'])[:2] == '08':
                    string_columna = "         " + string_columna
                elif str(query_anexo3_procesada.loc[q, 'Periodo'])[:2] == '11':
                    string_columna = "       " + string_columna
                elif str(query_anexo3_procesada.loc[q, 'Periodo'])[:2] == '09':
                    string_columna = "           " + string_columna
                elif str(query_anexo3_procesada.loc[q, 'Periodo'])[:2] == '12':
                    string_columna = "  " + string_columna
                elif str(query_anexo3_procesada.loc[q, 'Periodo'])[:2] != '01':
                    string_columna = "   " + string_columna

                # if query_anexo3_procesada.loc[q,'Salidas_periodo']<10:
                #   string_columna2 = "  " + string_columna2

                mensaje_telegram = (mensaje_telegram +
                                    str(query_anexo3_procesada.loc[q, 'Periodo']) +
                                    string_columna +
                                    str(query_anexo3_procesada.loc[q, 'Salidas_periodo']) +
                                    "\n")

        mensaje_telegram = (mensaje_telegram + " \nTotal salidas:    " +
                            str(query_anexo3['N° Salidas'].sum()) + "\n")
        mensaje_telegram = (mensaje_telegram + "Moda Distancia Base: " +
                            str(query_anexo3['Distancia Base (Km)'].mode()[0]) + "\n")
        mensaje_telegram = (mensaje_telegram + "Moda Distancia Total: " +
                            str(query_anexo3['Distancia Total (POB+POI) (Km)'].mode()[0]))
    else:
        mensaje_telegram = "No se consultó Anexo 3 ya que están malos los parámetros de la query"

    return mensaje_telegram


# interpreta argumentos que envia usuario a traves del bot para consultar anexo 3
def procesar_argumento_comando_anexo3(argumentos):
    interpretacion = ""
    servicio_i = ""
    sentido_i = "i"
    tipo_dia_i = "l"
    periodio_i = "ts"

    if argumentos:
        argumentos = [x.strip() for x in argumentos]

        if argumentos[0].upper() in servicios_STP:
            servicio_i = argumentos[0].upper()
            if len(argumentos) > 1:
                if argumentos[1][0].lower() in ['i', 'r']:
                    sentido_i = argumentos[1][0].lower()
                elif argumentos[1][0].lower() in ['l', 's', 'd']:
                    tipo_dia_i = argumentos[1][0].lower()
                elif argumentos[1][0].lower() in ['m', 't'] and len(argumentos[1]) > 1:
                    periodio_i = argumentos[1][:2].lower()

                if len(argumentos) > 2:
                    if argumentos[2][0].lower() in ['l', 's', 'd']:
                        tipo_dia_i = argumentos[2][0].lower()
                    elif argumentos[2][0].lower() in ['m', 't'] and len(argumentos[2]) > 1:
                        periodio_i = argumentos[2][:2].lower()

                    if len(argumentos) > 3:
                        if argumentos[3][0].lower() in ['m', 't'] and len(argumentos[3]) > 1:
                            periodio_i = argumentos[3][:2].lower()
        else:
            interpretacion = ("Ese no me lo sabía, servicio tiene que estar con código " +
                              "transantiago, ejemplos: F53e, F52N, F01c, F02")
    else:
        interpretacion = ("Intentalo de nuevo. Escribe un servicio y alguna de las opciones en " +
                          "paréntesis [ ]: /anexo3 codigo_servicio_TS [i,r] [l,s,d] [mh,ts]")

    if servicio_i:
        interpretacion = ("Asumiendo que quisiste decir:\n /anexo3 " + servicio_i + " " +
                          sentido_i + " " + tipo_dia_i + " " + periodio_i)

    return [interpretacion, servicio_i, sentido_i, tipo_dia_i, periodio_i]


def consultar_donde_esta_ppu(ppu_q=""):
    q_ppu = ppu_q[:4] + "-" + ppu_q[-2:]
    db1 = MySQLdb.connect(host=ip_bd_edu,
                          user="brunom",
                          passwd="Manzana",
                          db="repositorios")

    cur1 = db1.cursor()

    cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido, fecha, hora, " +
                 "servicio_sentido_a_bordo_del_bus, estado, ubicacion, lugar, estado_ignicion " +
                 "FROM ultimas_transmisiones WHERE patente = '" + q_ppu + "' ;")

    datos = [row for row in cur1.fetchall() if row[1] is not None]
    cur1.close()
    db1.close()

    if datos:
        datos = datos[0]
        mensaje_telegram = ("Columnas de última transmisión\n" + datos[3] + " " + datos[4] +
                            " - " + str(datos[5]) + " " + str(datos[6]) + "\n" + datos[7] +
                            " - " + datos[8] + "\n" + datos[9] + " - " + str(datos[10]) +
                            "\n" + datos[11])

        return [float(datos[1]), float(datos[2]), mensaje_telegram]
    else:
        return False


def consultar_donde_esta_ppu_maipu(ppu_q=""):
    q_ppu = ppu_q[:4] + "-" + ppu_q[-2:]
    db1 = MySQLdb.connect(host=ip_bd_edu,
                          user="brunom",
                          passwd="Manzana",
                          db="repositorios")

    cur1 = db1.cursor()

    cur1.execute("SELECT patente, latitudgps, longitudgps, servicio, sentido, fecha, hora, " +
                 "servicio_sentido_a_bordo_del_bus, estado, ubicacion, lugar, estado_ignicion " +
                 "FROM ultimas_transmisiones_c WHERE patente = '" + q_ppu + "' ;")

    datos = [row for row in cur1.fetchall() if row[1] is not None]
    cur1.close()
    db1.close()

    if datos:
        datos = datos[0]
        mensaje_telegram = ("Columnas de última transmisión\n" + datos[3] + " " + datos[4] +
                            " - " + str(datos[5]) + " " + str(datos[6]) + "\n" + datos[7] +
                            " - " + datos[8] + "\n" + datos[9] + " - " + str(datos[10]) +
                            "\n" + datos[11])

        return [float(datos[1]), float(datos[2]), mensaje_telegram]
    else:
        return False


def consultar_pato():
    return "Anexo 8"


def start(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                     text="Hola, para empezar a usar este bot debes saber la password. " +
                          "Si tienes dudas sobre cómo ocuparlo mándame el mensaje /ayuda")


def ayuda(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        print(("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
               lista_acceso_dic[str(update.effective_user.id)] + ": /ayuda"))
        bot.send_message(chat_id=update.message.chat_id,
                         text=("Este bot (version " + n_version +
                               ") toma datos GPS y del planillón online para entregar " +
                               "información en tiempo real sobre los buses de STP, además puede " +
                               "entregar información del Plan Operacional. " +
                               "Para consultar puedes enviar:\n" +
                               "/comandos - enlista todos los comandos posibles\n" +
                               "/version - dice qué versión del bot está siendo usada\n" +
                               "/f94_104 - dice en cuantos minutos llegan los F94 hacia " +
                               "metro Los Leones.En caso que un bus se encuentre fuera de " +
                               "las dos rutas posibles, Vespucio-Tobalaba y Macul-Leones, " +
                               "no se estima el tiempo y se pone 'FR'\n" +
                               "/busesLL - dice cuantos buses se encuentran muy cerca del " +
                               "cabezal Metro Los Leones y cuáles se encuentran detenidos\n" +
                               "/busesEP - dice cuantos buses se encuentran muy cerca del " +
                               "cabezal El Peñón y cuáles se encuentran detenidos\n" +
                               "NOTA: En caso de que un bus no se ingresó al Planillón Online, " +
                               "no se puede saber para cual servicio es el F94, de todas formas " +
                               "se muestran estos buses con un 'NA' en la columna Servicio\n" +
                               "/uGPS_Electricos - dice patentes de los 25 buses Electricos, " +
                               "la hora de su última transmisión GPS y sus columnas " +
                               "'servicio_sentido_a_bordo_del_bus' (abreviada) y " +
                               "'Ubicación' del webservice\n" +
                               "/uGPS_10 - dice patentes de 10 buses al azar, la hora de su " +
                               "última transmisión GPS y su columna Ubicación del webservice\n" +
                               "/patentes_maipu - dice cuantas iniciales de patentes hay " +
                               "transmitiendo en la tabla de últimas transmisiones\n" +
                               "/anexo3 - dice salidas según anexo 3, por ejemplo " +
                               "'/anexo3 F53e i l mh' consulta salidas por media hora(mh) " +
                               "día laboral(l) sentido ida(i) del F53e\n" +
                               "/donde - dice donde está una PPU y entrega columnas webservice " +
                               "(servicio-sentido-fecha-hora-SSAB-estado-ubicacion), " +
                               "ejemplo /donde FLXT33\n" +
                               "/donde_maipu - dice donde está una PPU del terminal de Maipú, " +
                               "ejemplo /donde BBZX38"))

    else:
        print(("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
               str(update.effective_user.id) + ": /ayuda (unauthorized)"))
        bot.send_message(chat_id=update.message.chat_id,
                         text=("No tienes permiso para usar el bot, habla con alguien de " +
                               "Operaciones para pedir ayuda sobre cómo tener acceso."))


def comandos(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        if str(update.effective_user.id) == id_bruno_stefoni:
            bot.send_message(chat_id=update.message.chat_id,
                             text=("/comandos\n/version\n/f94_104\n/busesLL\n/busesEP\n" +
                                   "/uGPS_Electricos\n/uGPS_10\n/anexo3\n/donde\n" +
                                   "/Rorro\n/Pato\n/Cerdo\n/reset_accesos .\n" +
                                   "/guardar_accesos .\n/manzana roja\n/orden66\n/stop\n" +
                                   "/patentes_maipu\n/donde_maipu\n" +
                                   "/ayuda\n/ayuda_nuevo_acceso"))
        else:
            bot.send_message(chat_id=update.message.chat_id,
                             text=("/comandos\n/version\n/f94_104\n/busesLL\n/busesEP\n" +
                                   "/uGPS_Electricos\n/uGPS_10\n/anexo3\n/donde\n" +
                                   "/donde_maipu\n/patentes_maipu\n" +
                                   "/ayuda"))
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def version(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                     text="Bot de Telegram de STP versión " +
                     n_version + " pensado para la Gerencia de Operaciones y Estudios \n" +
                     "Hecho por Bruno Stefoni")


def ayuda_nuevo_acceso(bot, update):
    if str(update.effective_user.id) == id_bruno_stefoni:
        texto = ('t.me/STPOperacionesBOT' + '\n' +
                 'Con telegram instalado ese link abre un chat al bot de operaciones' + '\n' +
                 'Para poder consultar debes registrar una cuenta ' +
                 '(con algún NOMBRE, por ejemplo: Diego), ' +
                 'solo basta con enviarle el siguiente texto al bot:' + '\n' +
                 '"/manzana roja NOMBRE"' + '\n' +
                 'Por ejemplo:' + '\n' +
                 '/manzana roja Diego' + '\n' +
                 'Un vez registrado, al enviar "/ayuda" se explican las consultas posibles')

        bot.send_message(chat_id=update.message.chat_id, text=texto)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def F94_104(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        bot.send_message(chat_id=update.message.chat_id, text="Consultando base de datos..")
        mensaje_a_enviar = consultar_fts_104()
        bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def busesLL(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        bot.send_message(chat_id=update.message.chat_id, text="Consultando base de datos..")
        mensaje_a_enviar = consultar_buses_cabezal_LosLeones()
        bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def busesEP(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        bot.send_message(chat_id=update.message.chat_id, text="Consultando base de datos..")
        mensaje_a_enviar = consultar_buses_cabezal_ElPenon()
        bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def uGPS_Electricos(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        bot.send_message(chat_id=update.message.chat_id, text="Consultando base de datos..")
        mensaje_a_enviar = consultar_ultima_transmision_electricos()
        bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def uGPS_10(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        bot.send_message(chat_id=update.message.chat_id, text="Consultando base de datos..")
        mensaje_a_enviar = consultar_ultima_transmision_10()
        bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def patentes_maipu(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        bot.send_message(chat_id=update.message.chat_id, text="Consultando base de datos..")
        mensaje_a_enviar = consultar_patentes_ultima_transmision_maipu()
        bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def n_registros(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        bot.send_message(chat_id=update.message.chat_id, text="Consultando base de datos..")
        mensaje_a_enviar = consultar_numero_ultimas_transmisiones()
        bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def n_registros_maipu(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        bot.send_message(chat_id=update.message.chat_id, text="Consultando base de datos..")
        mensaje_a_enviar = consultar_numero_ultimas_transmisiones_maipu()
        bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def anexo3(bot, update, args):
    if str(update.effective_user.id) in lista_acceso_dic:
        args_procesado = procesar_argumento_comando_anexo3(args)
        bot.send_message(chat_id=update.message.chat_id, text=args_procesado[0])
        if args_procesado[1]:
            bot.send_message(chat_id=update.message.chat_id, text="Consultando Anexo 3..")
            mensaje_a_enviar = consultar_anexo3(
                args_procesado[1], args_procesado[2], args_procesado[3], args_procesado[4])
            bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def donde(bot, update, args):
    if str(update.effective_user.id) in lista_acceso_dic:
        if args:
            print(("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
                   lista_acceso_dic[str(update.effective_user.id)] + ": /donde " + args[0]))
            if len(args[0].strip()) == 6:
                latlon_consulta = consultar_donde_esta_ppu(args[0].strip().upper())
                if latlon_consulta:
                    bot.send_message(chat_id=update.message.chat_id, text=latlon_consulta[2])
                    bot.send_location(chat_id=update.message.chat_id,
                                      latitude=latlon_consulta[0], longitude=latlon_consulta[1])

                else:
                    bot.send_message(chat_id=update.message.chat_id,
                                     text=("No se encontró la PPU " + args[0].strip().upper() +
                                           " quizá la PPU no existe, falló la conexión o es " +
                                           "una PPU del terminal de maipú, en el última caso " +
                                           "probar con comando /donde_maipu"))
            else:
                bot.send_message(chat_id=update.message.chat_id,
                                 text=("Tiene que ser una ppu sin guiones, " +
                                       "por ejemplo /donde FLXT33"))
        else:
            bot.send_message(chat_id=update.message.chat_id,
                             text=("Enviar una ppu sin guiones con el comando, " +
                                   "por ejemplo /donde FLXT33"))
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def donde_maipu(bot, update, args):
    if str(update.effective_user.id) in lista_acceso_dic:
        if args:
            print(("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
                   lista_acceso_dic[str(update.effective_user.id)] + ": /donde_maipu " +
                   args[0]))
            if len(args[0].strip()) == 6:
                latlon_consulta = consultar_donde_esta_ppu_maipu(args[0].strip().upper())
                if latlon_consulta:
                    bot.send_message(chat_id=update.message.chat_id, text=latlon_consulta[2])
                    bot.send_location(chat_id=update.message.chat_id,
                                      latitude=latlon_consulta[0], longitude=latlon_consulta[1])

                else:
                    bot.send_message(chat_id=update.message.chat_id,
                                     text="No se encontró la PPU " + args[0].strip().upper() +
                                          " quizá la PPU no existe o falló la conexión")
            else:
                bot.send_message(chat_id=update.message.chat_id,
                                 text=("Tiene que ser una ppu sin guiones, " +
                                       "por ejemplo /donde FLXT33"))
        else:
            bot.send_message(chat_id=update.message.chat_id,
                             text=("Enviar una ppu sin guiones con el comando, " +
                                   "por ejemplo /donde FLXT33"))
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def Rorro(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        if lista_acceso_dic[str(update.effective_user.id)]:
            if lista_acceso_dic[str(update.effective_user.id)][0].upper() == 'R':
                bot.send_message(chat_id=update.message.chat_id,
                                 text="Hola 🐽 Rorro, descartando datos..")
                mensaje_a_enviar = consultar_rorro()
                bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
            else:
                bot.send_message(chat_id=update.message.chat_id, text="Descartando datos..")
                mensaje_a_enviar = consultar_rorro()
                bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
        else:
            bot.send_message(chat_id=update.message.chat_id, text="Descartando datos..")
            mensaje_a_enviar = consultar_rorro()
            bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def Pato(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        bot.send_message(chat_id=update.message.chat_id, text="Consultando base de datos garca..")
        mensaje_a_enviar = consultar_pato()
        bot.send_message(chat_id=update.message.chat_id, text=mensaje_a_enviar)
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def cerdo(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        bot.send_message(chat_id=update.message.chat_id, text="Lo más " +
                         lista_acceso_dic[str(update.effective_user.id)] + " cerdo asqueroso 🐷")
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def reset_accesos(bot, update, args):
    if args:
        if str(update.effective_user.id) == id_bruno_stefoni:
            lista_acceso_dic.clear()
            lista_acceso_dic[id_bruno_stefoni] = "Bruno"
            bot.send_message(chat_id=update.message.chat_id,
                             text="Hola Bruno, se reseteó el diccionario con accesos")
        else:
            bot.send_message(chat_id=update.message.chat_id,
                             text="Solo Bruno puede resetear accesos")


def guardar_accesos(bot, update, args):
    if args:
        if str(update.effective_user.id) == id_bruno_stefoni:
            outfile = open('lista_acceso.json', 'w')
            json.dump(lista_acceso_dic, outfile, indent=4)
            outfile.close()
            bot.send_message(chat_id=update.message.chat_id,
                             text=("Hola Bruno, " +
                                   "se guardó el diccionario con los accesos actualizados"))
        else:
            bot.send_message(chat_id=update.message.chat_id,
                             text="Solo Bruno puede guardar los accesos actuales")


def manzana(bot, update, args):
    if args:
        if args[0].strip().lower() == "roja":
            if len(args) > 1:
                if str(update.effective_user.id) in lista_acceso_dic:
                    bot.send_message(chat_id=update.message.chat_id,
                                     text="Ya te conocía con otro nombre: " +
                                          lista_acceso_dic[str(update.effective_user.id)])
                    bot.send_message(chat_id=update.message.chat_id,
                                     text=("Voy a intentar cambiar tu nombre de mi registro.."))
                elif len(args) > 1:
                    bot.send_message(chat_id=update.message.chat_id,
                                     text="No te conocía, te agregaré a mi registro de usuarios " +
                                          "con el nombre que me has proporcionado..")
                if len(args) < 7:
                    argumentos = ""
                    for arg in args[1:]:
                        argumentos = argumentos + arg + " "
                    argumentos = argumentos[:-1]

                    if len(argumentos) < 60:
                        lista_acceso_dic[str(update.effective_user.id)] = argumentos
                        bot.send_message(chat_id=update.message.chat_id,
                                         text="Te agregué con el nombre " + argumentos)
                    else:
                        bot.send_message(chat_id=update.message.chat_id,
                                         text=("No te agregué con el nombre dado, " +
                                               "debes ingresar menos de 60 caracteres"))
                else:
                    bot.send_message(chat_id=update.message.chat_id,
                                     text=("No te agregué con el nombre dado, " +
                                           "debes ingresar máximo 6 palabras"))
            else:
                bot.send_message(chat_id=update.message.chat_id,
                                 text=("No has proporcionado ningún nombre, " +
                                       "si quieres intenta de nuevo con\n/manzana roja algun_nombre"))

    if str(update.effective_user.id) in lista_acceso_dic:
        print(("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
              lista_acceso_dic[str(update.effective_user.id)] + ": /manzana"))
    else:
        print(("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
              str(update.effective_user.id) + ": /manzana"))


def shutdown():
    if updater.is_idle:
        updater.is_idle = False
        updater.stop()


def stop(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        print(("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
              lista_acceso_dic[str(update.effective_user.id)] + ": /stop "))

        if seguro_telegramearlyexit:
            threading.Thread(target=shutdown).start()
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def orden66(bot, update):
    if str(update.effective_user.id) in lista_acceso_dic:
        global seguro_telegramearlyexit
        seguro_telegramearlyexit = True
        bot.send_message(chat_id=update.message.chat_id, text="Orden lista")
    else:
        bot.send_message(chat_id=update.message.chat_id, text="Acceso denegado.")


def main():

    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler('start', start))
    dispatcher.add_handler(CommandHandler('ayuda', ayuda))
    dispatcher.add_handler(CommandHandler('comandos', comandos))
    dispatcher.add_handler(CommandHandler('version', version))
    dispatcher.add_handler(CommandHandler('F94_104', F94_104))
    dispatcher.add_handler(CommandHandler('busesLL', busesLL))
    dispatcher.add_handler(CommandHandler('busesEP', busesEP))

    dispatcher.add_handler(CommandHandler('uGPS_Electricos', uGPS_Electricos))
    dispatcher.add_handler(CommandHandler('uGPS_10', uGPS_10))
    dispatcher.add_handler(CommandHandler('patentes_maipu', patentes_maipu))

    dispatcher.add_handler(CommandHandler('n_registros', n_registros))
    dispatcher.add_handler(CommandHandler('n_registros_maipu', n_registros_maipu))

    dispatcher.add_handler(CommandHandler('anexo3', anexo3, pass_args=True))
    dispatcher.add_handler(CommandHandler('donde', donde, pass_args=True))
    dispatcher.add_handler(CommandHandler('donde_maipu', donde_maipu, pass_args=True))

    dispatcher.add_handler(CommandHandler('Pato', Pato))
    dispatcher.add_handler(CommandHandler('Rorro', Rorro))
    dispatcher.add_handler(CommandHandler('cerdo', cerdo))

    dispatcher.add_handler(CommandHandler('reset_accesos', reset_accesos, pass_args=True))
    dispatcher.add_handler(CommandHandler('guardar_accesos', guardar_accesos, pass_args=True))
    dispatcher.add_handler(CommandHandler('manzana', manzana, pass_args=True))

    dispatcher.add_handler(CommandHandler('stop', stop))
    dispatcher.add_handler(CommandHandler('orden66', orden66))

    dispatcher.add_handler(CommandHandler('ayuda_nuevo_acceso', ayuda_nuevo_acceso))

    updater.start_polling(poll_interval=3)
    print("Bot " + n_version + " operativo")
    updater.idle()
    print("Cerrando bot..")


if __name__ == '__main__':
    print("Iniciando bot de telegram..")
    main()
    print("Programa bot de telegram finalizado")
