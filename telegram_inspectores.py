import MySQLdb
from telegram.ext import Updater, CommandHandler, MessageHandler
import pandas as pd
import datetime as dt
import json
from geopy import distance
import threading

n_version = "6.0"
ip_webservice = "192.168.11.199"
ip_bd_edu = "192.168.11.150"

id_bruno_stefoni = "421833900"
infile = open('fts104_10metros_modificado_contiempo.json', 'r')
ruta_fts104_10metros = json.load(infile)
infile.close()

delta_hacia_atras = 5
criterio_spam = dt.timedelta(seconds=60)

seguro_telegramearlyexit = False
LosLeones_centroide = [-33.4207887, -70.6079092]
ElPennon_centroide = [-33.578444, -70.551972]

servicios_validos_FTS = ['F90', 'F91', 'F92', 'F93', 'F94', 'F95', 'FTS']
df_ppu_lostilos = pd.read_excel('ppu_los_tilos.xlsx')
patentesL_lostilos = df_ppu_lostilos.PPU.to_numpy().tolist()
patentesL_electricos = ['LCTG-23', 'LCTG-24', 'LCTG-25', 'LCTG-26', 'LCTG-27', 'LCTG-28', 'LCTG-29',
                        'LCTG-30', 'LCTG-31', 'LCTG-32', 'LCTG-33', 'LCTG-34', 'LCTG-35', 'LCTG-36',
                        'LCTG-37', 'LCTG-38', 'LCTG-39', 'LCTG-40', 'LCTG-41', 'LCTG-42', 'LCTG-43',
                        'LCTG-44', 'LCTG-45', 'LCTG-46', 'LCTG-47']

global ahora_ultima_queryf94_104
global ahora_ultima_querybusesll
global ahora_ultima_querybusesep

global mensaje_ultima_queryf94_104
global mensaje_ultima_querybusesll
global mensaje_ultima_querybusesep

global primera_queryf94_104
global primera_querybusesll
global primera_querybusesep

ahora_ultima_queryf94_104 = dt.datetime.now().replace(microsecond=0)
ahora_ultima_querybusesll = dt.datetime.now().replace(microsecond=0)
ahora_ultima_querybusesep = dt.datetime.now().replace(microsecond=0)

mensaje_ultima_queryf94_104 = "."
mensaje_ultima_querybusesll = ".."
mensaje_ultima_querybusesep = "..."

primera_queryf94_104 = True
primera_querybusesll = True
primera_querybusesep = True


#   SERVIDOR CHICO
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

cur0.close()
db0.close()


TOKEN = "838619024:AAEbif7W-ZS4OKJA0W1MFS8Il3-1jptsx7s"  # llave stp100
updater = Updater(token=TOKEN)


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


def sacar_tiempo_restante_vespucio(tiempo_mediahora, d, indice_min):
    return tiempo_mediahora[3] * d[indice_min] / d[0]


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


def pendiente(a, b):
    if a[0] - b[0] == 0:
        return 0
    else:
        return (a[1] - b[1]) / (a[0] - b[0])


def corte(p, g):
    return g[1] - g[0] * p * 1.0


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


def mensaje_tiempo_estimado(tiempo_estimado):
    if tiempo_estimado > 997:
        return "FR"

    return str(int((int(tiempo_estimado) + 1)))


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

        for row in datosOK:
            row[6] = dt.datetime.combine(row[5], (dt.datetime.min + row[6]).time())

        datos_FTS = [row for row in datosOK if row[6] > ahora_delta and
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

        for row in datosOK:
            row[6] = dt.datetime.combine(row[5], (dt.datetime.min + row[6]).time())

        datos_cabezal = [row for row in datosOK if row[6] > ahora_delta and
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

        for row in datosOK:
            row[6] = dt.datetime.combine(row[5], (dt.datetime.min + row[6]).time())

        datos_cabezal = [row for row in datosOK if row[6] > ahora_delta and
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


def start(bot, update):
    chatID = update.message.chat_id
    bot.send_message(chat_id=chatID, text="Hola, si tienes dudas mándame el mensaje /ayuda")
    print("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
          str(update.effective_user.id) + ": /start")


def ayuda(bot, update):
    chatID = update.message.chat_id
    bot.send_message(chat_id=chatID,
                     text="Este bot toma datos GPS y del planillón online para decir en " +
                     "cuanto llegan los buses F94 que van hacia metro Los Leones\n" +
                     " Comandos:\n" +
                     "/version - dice qué versión del bot está siendo usada\n" +
                     "/f94_104 - dice en cuantos minutos llegan los F94 hacia metro Los Leones. " +
                     "En caso que un bus se encuentre fuera de las dos rutas posibles, " +
                     "Vespucio-Tobalaba y Macul-Leones, no se estimará " +
                     "el tiempo y se enviará 'FR'\n" +
                     "/busesLL - dice cuantos buses se encuentran muy cerca del cabezal " +
                     "Metro Los Leones y cuáles se encuentran detenidos\n" +
                     "/busesEP - dice cuantos buses se encuentran muy cerca del cabezal " +
                     "El Peñón y cuáles se encuentran detenidos\n" +
                     "NOTA: En caso que un bus en ruta no se ingresó al Planillón Online, " +
                     "no se puede saber para cual servicio es el F94, de todas formas se " +
                     "muestran estos buses con un 'NA' en la columna Servicio")
    print("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
          str(update.effective_user.id) + ": /ayuda")


def version(bot, update):
    chatID = update.message.chat_id
    bot.send_message(chat_id=chatID, text="Bot de Telegram de STP versión " + n_version +
                     " pensado para servicios 100 \n" + "Hecho por Bruno Stefoni")
    print("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
          str(update.effective_user.id) + ": /version")


def F94_104(bot, update):
    chatID = update.message.chat_id
    bot.send_message(chat_id=chatID, text="Consultando base de datos..")
    mensaje_a_enviar = consultar_fts_104()
    bot.send_message(chat_id=chatID, text=mensaje_a_enviar)
    print("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
          str(update.effective_user.id) + ": /f94_104")


def busesLL(bot, update):
    chatID = update.message.chat_id
    bot.send_message(chat_id=chatID, text="Consultando base de datos..")
    mensaje_a_enviar = consultar_buses_cabezal_LosLeones()
    bot.send_message(chat_id=chatID, text=mensaje_a_enviar)
    print("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
          str(update.effective_user.id) + ": /busesLL")


def busesEP(bot, update):
    chatID = update.message.chat_id
    bot.send_message(chat_id=chatID, text="Consultando base de datos..")
    mensaje_a_enviar = consultar_buses_cabezal_ElPenon()
    bot.send_message(chat_id=chatID, text=mensaje_a_enviar)
    print("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
          str(update.effective_user.id) + ": /busesEP")


def shutdown():
    if updater.is_idle:
        updater.is_idle = False
        updater.stop()


def stop(bot, update):
    if seguro_telegramearlyexit:
        threading.Thread(target=shutdown).start()
    print("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
          str(update.effective_user.id) + ": /stop")


def orden66(bot, update):
    if str(update.effective_user.id) == id_bruno_stefoni:
        global seguro_telegramearlyexit
        seguro_telegramearlyexit = True
        bot.send_message(chat_id=update.message.chat_id, text="Orden lista")
    print("[" + dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " +
          str(update.effective_user.id) + ": /orden66")


def main():

    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler('start', start))
    dispatcher.add_handler(CommandHandler('ayuda', ayuda))
    dispatcher.add_handler(CommandHandler('version', version))
    dispatcher.add_handler(CommandHandler('F94_104', F94_104))
    dispatcher.add_handler(CommandHandler('busesLL', busesLL))
    dispatcher.add_handler(CommandHandler('busesEP', busesEP))
    dispatcher.add_handler(CommandHandler('stop', stop))
    dispatcher.add_handler(CommandHandler('orden66', orden66))

    updater.start_polling(poll_interval=3)
    print("Bot operativo")
    updater.idle()
    print("Cerrando bot y conexiones a bases de datos..")


if __name__ == '__main__':
    print("Iniciando bot de telegram..")
    main()
    print("Programa bot de telegram finalizado")
