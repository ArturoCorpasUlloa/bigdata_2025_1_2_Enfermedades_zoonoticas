import sqlite3
import pandas as pd
import os # Para manejar rutas de forma más robusta

class DataBase:
    def __init__(self, db_name="enfermedades_zoonoticas.sqlite"):
        # Construir la ruta a la base de datos de forma más robusta
        # Asumimos que la carpeta 'db' está en 'src/static/db/'
        # y que este script (database.py) está en 'src/'
        base_dir = os.path.dirname(os.path.abspath(__file__)) # Directorio de database.py (src)
        self.db_path = os.path.join(base_dir, 'static', 'db', db_name)
        
        # Asegurarse de que el directorio de la base de datos exista
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        print(f"Ruta de la base de datos: {self.db_path}")


    def _connect(self):
        """Crea y retorna una conexión a la base de datos."""
        return sqlite3.connect(self.db_path)

    def insert_data(self, df: pd.DataFrame, nom_table: str = "enfermedades_zoonoticas"):
        if df.empty:
            print(f"DataFrame para la tabla '{nom_table}' está vacío. No se insertarán datos.")
            return
        try:
            df_copy = df.copy() # Trabajar con una copia para no modificar el original
            conn = self._connect()
            # if_exists='replace' borrará la tabla y la creará de nuevo.
            # if_exists='append' añadiría los datos si la tabla ya existe.
            df_copy.to_sql(name=nom_table, con=conn, if_exists='replace', index=False)
            conn.close()
            print(f"Datos insertados correctamente en la tabla '{nom_table}'. {len(df_copy)} filas añadidas/reemplazadas.")
        except Exception as e:
            print(f"Error al guardar los datos en la tabla '{nom_table}': {e}")

    def read_data(self, nom_table: str = "enfermedades_zoonoticas", query: str = None):
        df = pd.DataFrame()
        try:
            conn = self._connect()
            if query is None:
                query = f"SELECT * FROM {nom_table}"
            df = pd.read_sql_query(sql=query, con=conn)
            conn.close()
            print(f"Consulta realizada en '{nom_table}': {query}")
            return df
        except sqlite3.OperationalError as e:
             print(f"Error al leer datos de la tabla '{nom_table}': {e}. ¿Existe la tabla?")
             return df # Retorna DataFrame vacío
        except Exception as e:
            print(f"Error inesperado al obtener los datos de la tabla '{nom_table}': {e}")
            return df # Retorna DataFrame vacío

    def update_data(self, nom_table: str, set_column: str, set_value, where_column: str, where_value):
        try:
            conn = self._connect()
            cursor = conn.cursor()
            # Usar placeholders (?) es más seguro contra inyección SQL
            query = f"UPDATE {nom_table} SET \"{set_column}\" = ? WHERE \"{where_column}\" = ?"
            cursor.execute(query, (set_value, where_value))
            conn.commit()
            rows_affected = cursor.rowcount
            conn.close()
            if rows_affected > 0:
                print(f"Datos actualizados correctamente en '{nom_table}'. {rows_affected} fila(s) afectada(s).")
            else:
                print(f"No se encontraron filas para actualizar en '{nom_table}' con {where_column} = {where_value}.")
        except Exception as e:
            print(f"Error al actualizar los datos en la tabla '{nom_table}': {e}")

    def delete_data(self, nom_table: str, where_column: str, where_value):
        try:
            conn = self._connect()
            cursor = conn.cursor()
            # Usar placeholders (?) es más seguro
            # Asegurarse de que el nombre de la columna esté entre comillas si puede contener espacios o palabras clave
            query = f"DELETE FROM {nom_table} WHERE \"{where_column}\" = ?"
            cursor.execute(query, (where_value,)) # where_value debe ser una tupla
            conn.commit()
            rows_affected = cursor.rowcount
            conn.close()
            if rows_affected > 0:
                print(f"Datos eliminados correctamente de '{nom_table}' donde {where_column} = {where_value}. {rows_affected} fila(s) eliminada(s).")
            else:
                print(f"No se encontraron filas para eliminar en '{nom_table}' con {where_column} = {where_value}.")
        except Exception as e:
            print(f"Error al eliminar los datos de la tabla '{nom_table}': {e}")

# Ejemplo de uso si se ejecuta este archivo directamente (para pruebas)
if __name__ == '__main__':
    print("Probando la clase DataBase...")
    # Crear una instancia de DataBase. El archivo .sqlite se creará en src/static/db/
    db_test = DataBase(db_name="test_database.sqlite")

    # Crear un DataFrame de prueba
    data = {
        'codigo': [1, 2, 3, 4],
        'enfermedad': ['Gripe Aviar', 'Rabia', 'Brucelosis', 'Leptospirosis'],
        'especie_afectada': ['Aves', 'Mamíferos', 'Bovinos', 'Roedores/Humanos']
    }
    test_df = pd.DataFrame(data)
    nombre_tabla_test = "enfermedades_test"

    print("\n--- Insertando datos ---")
    db_test.insert_data(test_df, nombre_tabla_test)

    print("\n--- Leyendo datos después de insertar ---")
    df_leido = db_test.read_data(nombre_tabla_test)
    print(df_leido)

    print("\n--- Actualizando datos (código 2) ---")
    db_test.update_data(nombre_tabla_test, "especie_afectada", "Todos los mamíferos", "codigo", 2)
    df_actualizado = db_test.read_data(nombre_tabla_test)
    print(df_actualizado)

    print("\n--- Eliminando datos (código 3) ---")
    db_test.delete_data(nombre_tabla_test, "codigo", 3)
    df_despues_eliminar = db_test.read_data(nombre_tabla_test)
    print(df_despues_eliminar)

    print("\n--- Intentando eliminar dato inexistente (código 99) ---")
    db_test.delete_data(nombre_tabla_test, "codigo", 99)
    df_final = db_test.read_data(nombre_tabla_test)
    print(df_final)
    
