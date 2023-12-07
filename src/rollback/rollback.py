import os



class Rollback():

    def __init__(self, parent_dir, id):
        
        self.output_path = os.path.join(parent_dir, "outputs")
        self.folder_path = os.path.join(self.output_path, id)
        print(self.folder_path)