import pandas as pd
import os


ORIGINAL_CSV = 'bitcoin_data.csv'
SORTED_CSV = 'bitcoin_data_sorted.csv'


# Carica dataset, lo ordina per data e a partire dal 2017 (maggiore volatilità prezzi bitcoin)
def sort_dataset_by_time():

    if not os.path.exists(ORIGINAL_CSV):
        print(f"ERRORE: File '{ORIGINAL_CSV}' non trovato.")
        return

    print(f"Caricamento del dataset da '{ORIGINAL_CSV}'...")
    # Carichiamo intero dataset
    df = pd.read_csv(ORIGINAL_CSV)
    print(f"Caricate {len(df)} righe.")

    # Filtriamo per dati più recenti (volatili)
    # Convertiamo il timestamp in formato data per poter filtrare
    df['datetime'] = pd.to_datetime(df['Timestamp'], unit='s')

    print("Filtraggio del dataset per mantenere i dati dal 2017 in poi...")
    df_filtered = df[df['datetime'] >= '2017-01-01']
    print(f"Mantenute {len(df_filtered)} righe dopo il filtraggio.")

    print("Ordinamento del dataset per timestamp...")
    # Ordiniamo il DataFrame in base alla colonna Timestamp
    df_sorted = df_filtered.sort_values(by='Timestamp', ascending=True)

    print(f"Salvataggio del dataset ordinato in '{SORTED_CSV}'...")
    #  salviamo nuovo df (index=False per non salvare la colonna indice di pandas)
    df_sorted.drop(columns=['datetime']).to_csv(SORTED_CSV, index=False)
    print(
        f"--- Preparazione dati completata. Ora puoi usare '{SORTED_CSV}' per lo streaming. ---")


if __name__ == "__main__":
    sort_dataset_by_time()
