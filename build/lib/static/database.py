import sqlite3
import pandas as pd

class DataBase:
    def __init__(self):"src/edu_bigdata/static/db/enfermedades_zoonoticas.sqlite"
    
    # CRUD C = create(insert) R= read U = update DF= Delete
    def insert_data(self,df = pd.DataFrame(),nom_table="enfermedades_zoonoticas"):
        try:
            df = df.copy()
            conn = sqlite3.connect(self.db_name)
            df.to_sql(name=nom_table,con=conn,if_exists='replace') # sobreescriba , inserte al final, actualizacion datos
            conn.close()
        except Exception as errores:
            print("error al guardar los datos")
    
    #Leer Los datos 
    def read_data(self,nom_table="enfermedades_zoonoticas"):
        df=pd.DataFrame()
        try:
            if len(nom_table)>0:
                conn = sqlite3.connect(self.db_name)
                query= "select * from {}".format(nom_table)
                df = pd.read_sql_query(sql=query,con=conn)
                print("*************** consulta base datos tabla: {}*********".format(query))
                conn.close
                return df
        except Exception as errores:
            print("error al obtener los datos")
            return df
        
    #Actualizar Datos
    def update_data(self, nom_table, set_column, set_value, where_column, where_value):
        try:
            conn = sqlite3.connect(self.db_name)
            query = f"UPDATE {nom_table} SET {set_column} = ? WHERE {where_column} = ?" 
            conn.execute(query, (set_value, where_value))
            conn.commit()
            conn.close()
            print("Datos actualizados correctamente.")
        except Exception as errores:
            print("Error al actualizar los datos:", errores)

    #Borrar Datos
    def delete_data(self, nom_table, where_column, where_value):
        try:
            conn = sqlite3.connect(self.db_name)
            query = f"DELETE FROM {nom_table} WHERE {where_column} = ?"
            conn.execute(query, (where_value,))
            conn.commit()
            conn.close()
            print("Datos eliminados correctamente.")
        except Exception as errores:
            print("Error al eliminar los datos:", errores)