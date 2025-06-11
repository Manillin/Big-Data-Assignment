import pandas as pd
import time
import os
import shutil

# --- Configurazione ---
CSV_FILE_PATH = 'bitcoin_data_sorted.csv'
STREAMING_DIR = 'streaming_data'
CHUNK_SIZE = 100  # Numero di righe da inviare in ogni batch
DELAY_SECONDS = 2  # Pausa in secondi tra un batch e l'altro


# Legge un file CSV a pezzi e simula uno stream di dati scrivendo i pezzi come file separati in una cartella di output
def simulate_stream():
    # Preparazione della cartella di streaming
    if os.path.exists(STREAMING_DIR):
        shutil.rmtree(STREAMING_DIR)
        print(f"Cartella '{STREAMING_DIR}' esistente rimossa.")

    os.makedirs(STREAMING_DIR)
    print(f"Cartella '{STREAMING_DIR}' creata.")

    #  Lettura del dataset di origine
    print(f"Inizio lettura del dataset da '{CSV_FILE_PATH}'...")
    try:
        # Usiamo un iterator per leggere il file a "pezzi" (chunk) senza
        # caricarlo tutto in memoria.
        data_iterator = pd.read_csv(
            CSV_FILE_PATH,
            iterator=True,
            chunksize=CHUNK_SIZE
        )
    except FileNotFoundError:
        print(
            f"ERRORE: File '{CSV_FILE_PATH}' non trovato. Assicurati che sia nella cartella corretta.")
        return  # Esce dalla funzione se il file non c'è

    print("\n--- Inizio della simulazione dello stream ---")
    print(
        f"Verrà scritto un file ogni {DELAY_SECONDS} secondi nella cartella '{STREAMING_DIR}'.")
    print("Puoi interrompere lo script in qualsiasi momento con Ctrl+C.")

    # Ciclo di simulazione
    batch_number = 0
    try:
        for chunk in data_iterator:
            batch_number += 1
            if not chunk.empty:
                output_filename = os.path.join(
                    STREAMING_DIR, f"data_batch_{batch_number:05d}.csv")

                # Scriviamo il chunk in un nuovo file CSV, includendo l'header.
                chunk.to_csv(output_filename, index=False, header=True)
                print(
                    f"Batch {batch_number}: Creato file '{output_filename}' con {len(chunk)} righe.")
            else:
                print(
                    f"Batch {batch_number}: Saltato perché il chunk era vuoto.")

            # timeout
            time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n\nSimulazione interrotta dall'utente.")
    except Exception as e:
        print(f"\nSi è verificato un errore: {e}")

    print("\n--- Simulazione dello stream completata (o interrotta) ---")


if __name__ == "__main__":
    simulate_stream()
