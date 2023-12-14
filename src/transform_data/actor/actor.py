import pandas as pd
from transform_data import TransformData
from enums import ExcelColumns
from pnn_monitoring_orm import Actor, Institution, Detail, Milestone, Guideline, Action

class ActorT(TransformData):

    def __init__(self, data, load):
        self.load = load
        self.root_dir = self.load.root_dir
        self.actu_date = self.load.actu_date

        super().__init__(data, self.root_dir, self.actu_date)
        
        self.column_name_actor = ExcelColumns.ACTOR.value 
        self.column_name_detail = ExcelColumns.DETAIL.value 
        self.column_name_milestone = ExcelColumns.MILESTONE.value
        self.log_error_file = "actor_error_log.txt"
        self.check_columns([self.column_name_actor, self.column_name_detail, self.column_name_milestone])
    
    def obtain_data_from_df(self):

        data_to_save = []
        institution_text = ""
        milestone_text = ""
        try:
            institutions_db = self.load.session.query(Institution.id, Institution.name).all()
            details_db = self.load.session.query(
                Detail.id,
                Detail.name,
                Milestone.id.label("milestone_id"),
                Milestone.name.label("milestone_name"),
                Guideline.sirap_id
            ).join(
                Milestone,
                Milestone.id == Detail.milestone_id
            ).join(
                Action,
                Action.id == Milestone.action_id
            ).join(
                Guideline,
                Guideline.id == Action.guideline_id
            ).all()
            if institutions_db and details_db :
                for index, row in self.data["data"].iterrows():
                    if(pd.notna(row[self.column_name_actor]) and not row[self.column_name_actor].isspace()):
                        actors=row[self.column_name_actor]
                        actors=actors.replace(" - ", ",")
                        actors=actors.replace("\n", ",").split(",")
                        actors=[actor for actor in actors if actor.strip()]
                        actors=[actor.strip() for actor in actors]
                        milestone_text = row[self.column_name_milestone] if pd.notna(row[self.column_name_milestone]) and row[self.column_name_milestone] else milestone_text
                        detail = row[self.column_name_detail]
                        detail_id = self.get_detail_id(detail,milestone_text, details_db)
                        for data in actors:
                            data = {'normalize': self.tools.normalize_text(data), 'original': data}
                            institution_text = data['original']
                            intitution_id = self.get_institution_id(institution_text, institutions_db)
                            if intitution_id != 0 and detail_id != 0:
                                data_to_save.append({'institution_id': intitution_id, 'detail_id': detail_id})
                            elif intitution_id == 0:
                                msg_error = f"El actor {institution_text} no se encuentra en la base de datos"
                                self.tools.write_log(msg_error, self.log_error_file)
                                print(msg_error)
                            elif detail_id == 0:
                                msg_error = f"El detalle {detail} no se encuentra en la base de datos"
                                self.tools.write_log(msg_error, self.log_error_file)
                                print(msg_error)
                            
                df_result = pd.DataFrame(data_to_save)
                df_result = df_result.drop_duplicates(subset=['institution_id', 'detail_id'])
                return df_result
            
            else:
                if not institutions_db:
                    msg_error = f"No hay instituciones en la base de datos con los que relacionar los actores"
                else:
                    msg_error = f"No hay detalles en la base de datos con los que relacionar los actores"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)

                return pd.DataFrame(columns=['name'])
    
        except Exception as e:

            msg_error = f"Error al intentar transformar los actores: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return pd.DataFrame(columns=['name'])
    
    def obtain_data_from_db(self):

        try: 

            existing_actor = self.load.session.query(Actor.institution_id, Actor.detail_id, Guideline.sirap_id).join(
                Detail,
                Detail.id == Actor.detail_id
            ).join(
                Milestone,
                Milestone.id == Detail.milestone_id
            ).join(
                Action,
                Action.id == Milestone.action_id
            ).join(
                Guideline,
                Guideline.id == Action.guideline_id
            ).all()
            return existing_actor

        except Exception as e:
            msg_error = f"Error en la tabla Actor al intentar obtener los datos: {str(e)}"
            self.tools.write_log(msg_error, self.log_error_file)
            print(msg_error)

            return None


    def get_institution_id(self, text, institution_db):
        normalized_institution_db = set((self.tools.normalize_text(row.name), row.id) for row in institution_db)
        text = self.tools.normalize_text(text)       
        matching_elements = [element for element in normalized_institution_db if text in element]
        if matching_elements:
            return matching_elements[0][1]
        else:
            return 0
        
    def get_detail_id(self, text, milestone_text, detail_db):
        normalized_detail_db = set((row.id, self.tools.normalize_text(row.name), row.milestone_id, self.tools.normalize_text(row.milestone_name), row.sirap_id) for row in detail_db)
        text = self.tools.normalize_text(text)
        milestone_text = self.tools.normalize_text(milestone_text)
        matching_elements = [element for element in normalized_detail_db if text in element and milestone_text in element and element[4] == self.data["id"]]
        if matching_elements:
            return matching_elements[0][0]
        else:
            return 0
        
    def run_actor(self):

        print("\nInicia la transformación de los actores")

        existing_actor = self.obtain_data_from_db()
        new_actor = self.obtain_data_from_df()

        print("Finalizada la transformación de los actores")

        if existing_actor is not None and not new_actor.empty:

            new_log = []
            existing_log = []
            log_data = []

            print("Inicia la carga de los actores")
            try:
                for index, row in new_actor.iterrows():
                    if (row["institution_id"], row["detail_id"], self.data["id"]) not in existing_actor:
                        actor = Actor(institution_id=int(row["institution_id"]), detail_id=int(row["detail_id"]))
                        self.load.add_to_session(actor)
                        new_log.append(row["institution_id"])
                        log_data.append(actor)
                    else:
                        existing_log.append({"Fila": index+1, 
                                             'Valor institucion':row["institution_id"],
                                             'Valor detalle':row["detail_id"],
                                             "Error": f"Este actor ya se encuentra en la base de datos"})

                if log_data:
                    self.load.load_to_db(log_data, self.data["sirap_name"])
                
                if len(existing_log) > 0:
                    self.tools.generate_csv_with_errors(existing_log, self.column_name_actor, self.data["sirap_name"])

                msg = f'''Carga de los actores exitosa
                Nuevas actores guardados: {len(new_log)}
                Actores ya existentes en la base de datos: {len(existing_log)}\n'''
                print(msg)

                self.tools.write_log(msg, "output.txt", True)

            except Exception as e:
                msg_error = f"Error al guardar los actores: {str(e)}\n"
                self.tools.write_log(msg_error, self.log_error_file)
                print(msg_error)