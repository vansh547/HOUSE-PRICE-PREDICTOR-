import os 
import zipfile
from abc import ABC, abstractmethod

import pandas as pd

class DataIngestor(ABC):
    @abstractmethod
    def ingest(self,file_path: str) -> pd.DataFrame:
        pass

class zipDataIngestor(DataIngestor):
    def ingest(self, file_path: str) -> pd.DataFrame:
        if not file_path.endswith('.zip'):
            raise ValueError(f"The file at {file_path} is not a valid zip file.")
        
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall("extracted_data")
        
        extracted_files= os.listdir("extracted_data")
        csv_files = [f for f in extracted_files if f.endswith('.csv')]

        if len(csv_files) == 0:
            raise ValueError("No CSV files found in the extracted zip file.")
        if len(csv_files) > 1:
            raise ValueError("Multiple CSV files found in the extracted zip file.")
        
        csv_path = os.path.join("extracted_data", csv_files[0])
        df = pd.read_csv(csv_path)

        return df

class dataingestor_factory:
    @staticmethod
    def ingest(file_extension: str) -> pd.DataFrame:
        if file_extension == '.zip':
            return zipDataIngestor()
        else:
            raise ValueError("Unsupported file type.")
        
if __name__ == "__main__":
    file_path = "C:\\Users\\Mahakaal\\Documents\\HOUSE PRICE\\data\\archive.zip"

    ingestor = dataingestor_factory.ingest(".zip")
    
    df = ingestor.ingest(file_path)

    print(df.head())
