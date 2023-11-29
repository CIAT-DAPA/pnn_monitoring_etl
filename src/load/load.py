

class LoadData():


    def __init__(self, session):
        self.session = session
        
    def add_to_session(self, data):
        self.session.add(data)

    def load_to_db(self):
        self.session.commit()
