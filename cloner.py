import pandas as pd
from databricks.sdk.runtime import spark
class TableCloner:
    def __init__(self,spark = spark):
        self.spark = spark
        print('Preparando para clonar...')
        self.df = pd.read_csv('/dbfs/mnt/landing-prod/metadata/control/adb/clone_tables/clone_tables.csv')
        self._prepare_df()
    
    def _prepare_df(self):
        print('Completando datos faltantes...')
        self.df['location_prod'] = self.df['path_delta']
        self.df['location_prod'] = self.df.location_prod.str.replace('dbfs:/mnt/landing/', 'dbfs:/mnt/landing-prod/')
        self.df['location_prod'] = self.df.location_prod.str.replace('dbfs:/mnt/trusted/', 'dbfs:/mnt/trusted-prod/')
        self.df['location_local'] = self.df['path_delta']

    def clone_table(self, database, tablename):
        print('Aplicando filtros...')
        df_final = self.df[
            (self.df["database"].str.contains("trusted|refined")) &
            (self.df["location_prod"].str.contains("dbfs:/mnt/trusted-prod/"))
        ]
        try:
            location_prod = df_final.loc[(df_final["database"] == database) & (df_final["tablename"] == tablename), "location_prod"].iloc[0]
            location_local = df_final.loc[(df_final["database"] == database) & (df_final["tablename"] == tablename), "location_local"].iloc[0]
            
            # Eliminar la tabla si existe
            print('Eliminando la tabla si existe en este entorno...')
            self.spark.sql(f"DROP TABLE IF EXISTS {database}.{tablename}")
            # Clonar la tabla

            print('Clonando tabla...')
            print(f'Tabla:{tablename}')
            print(f'DB:{database}')
            print(f'PATH DELTA PROD:{location_prod}')
            print(f'Se almacenara en:{location_local}')


            self.spark.sql(f"""CREATE OR REPLACE TABLE {database}.{tablename} SHALLOW CLONE delta.`{location_prod}` LOCATION "{location_local}" """)
            print('Tabla clonada exitosamente!')
        except Exception as e:
            raise ValueError(f"Error al clonar la tabla: {str(e)}")
            
    def clone_all_tables(self):
        # Aplicar los mismos filtros que usaste en clone_table para obtener df_final
        df_final = self.df[
            (self.df["database"].str.contains("trusted|refined")) &
            (self.df["location_prod"].str.contains("dbfs:/mnt/trusted-prod/"))
        ]

        for _, row in df_final.iterrows():
            database = row["database"]
            tablename = row["tablename"]
            try:
                print(f"Clonando {database}.{tablename}...")
                self.clone_table(database, tablename)
            except Exception as e:
                print(f"Error al clonar {database}.{tablename}: {str(e)}")
                continue
        print("¡Todas las tablas han sido clonadas!")
