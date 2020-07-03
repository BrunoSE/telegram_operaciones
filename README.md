# bot de telegram

Este script es un bot de telegram para usarse en la Gerencia de Operaciones y Estudios de STP

## Detalles

Este bot responde a un usuario que debe registrarse, ofrece información en tiempo real del estado de la operación de buses de STP.

```
update.effective_user.id: corresponde al ID del usuario, sirve por ejemplo para darle privilegios de admin a una cierta ID
```

### Requerimientos
python 3\
pip install python-telegram-bot\
pip install pandas\
pip install mysqlclient\
pip install psycopg2\
pip install folium\
pip install geopy\
 \
fts104_10metros_modificado_contiempo.json\
lista_acceso.json\
Anexo 3 en excel\
 \
Una llave o Token que se obtuvo pidiendola dentro de la aplicación de telegram mandando un mensaje a la cuenta @BotFather
