# Analisi di Dati Finanziari in Tempo Reale con Apache Spark

## Descrizione del Progetto

Questo repository contiene un'applicazione di analisi dati in tempo reale che utilizza Apache Spark Structured Streaming. Il progetto implementa una pipeline End-to-End che:
1.  Simula un flusso di dati finanziari leggendo una serie storica dei prezzi di Bitcoin.
2.  Elabora i dati in micro-batch utilizzando Spark Structured Streaming.
3.  Calcola metriche aggregate (prezzo medio, minimo, massimo, volatilità e volume) su finestre temporali di 10 minuti.
4.  Implementa una logica di business per identificare e segnalare picchi di prezzo anomali.
5.  Salva i risultati in modo persistente e tollerante ai guasti su sink multipli in formato Apache Parquet.

## Architettura della Pipeline

Il sistema è basato su un'architettura producer-consumer:

-   **Data Preparer (`prepare_data.py`):** Uno script eseguito una sola volta per ordinare cronologicamente il dataset sorgente, un requisito fondamentale per le analisi di serie temporali con watermark.
-   **Producer (`stream_simulator.py`):** Uno script Python che legge il dataset ordinato e simula un flusso di dati scrivendo piccoli file CSV in una directory di input a intervalli regolari.
-   **Consumer (`spark_streaming_app.py`):** L'applicazione Spark principale che monitora la directory di input. Legge i nuovi dati, esegue le aggregazioni e la logica di alerting, e scrive i risultati su due destinazioni (sink) separate.
-   **Data Sinks (Filesystem):**
    -   `results_data/`: Contiene tutti i dati aggregati, salvati in formato Parquet.
    -   `results_alerts/`: Contiene solo i record che hanno attivato un alert, salvati in formato Parquet.
    -   `checkpoints_data/` e `checkpoints_alerts/`: Directory utilizzate da Spark per salvare lo stato dello stream, garantendo la tolleranza ai guasti (fault-tolerance).

## Stack Tecnologico

-   **Apache Spark 3.5.1**
-   **PySpark**
-   **Python 3.x**
-   **Pipenv** per la gestione dell'ambiente virtuale
-   **Pandas** per la preparazione e simulazione dei dati

---

## Prerequisiti

Prima di procedere, è necessario che i seguenti componenti siano installati e configurati nel sistema:

1.  **Java Development Kit (JDK):** Spark viene eseguito sulla JVM. Si raccomanda una versione LTS come Java 11 o 17.
2.  **Python 3.x**.
3.  **Pipenv:** Se non presente, può essere installato tramite `pip`:
    ```bash
    pip install pipenv
    ```

---

## Guida all'Installazione e Configurazione

Seguire i passaggi seguenti per configurare l'ambiente di progetto.

### 1. Clonazione del Repository

Clonare il repository sulla propria macchina locale.
```bash
git clone https://github.com/Manillin/Big-Data-Assignment
```

### 2. Installazione e Configurazione di Apache Spark

Questa applicazione richiede un'installazione locale di Apache Spark.

1.  **Scaricare Spark:** Scaricare la versione **Spark 3.5.1**, pre-compilata per **Hadoop 3**. È disponibile nell'archivio ufficiale:
    [**Download Spark 3.5.1**](https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz)

2.  **Estrarre l'archivio:** Decomprimere il file `.tgz` scaricato in una directory stabile (es. la propria cartella `Home` o `/opt/`).

3.  **Configurare le Variabili d'Ambiente:** È necessario informare il sistema operativo del percorso di installazione di Spark. Modificare il file di configurazione della shell (es. `~/.bash_profile` per Bash o `~/.zshrc` per Zsh) aggiungendo le seguenti righe.

    **Nota:** Sostituire `/percorso/assoluto/spark-3.5.1-bin-hadoop3` con il percorso reale della cartella in cui è stato estratto Spark.

    ```bash
    export SPARK_HOME="/percorso/assoluto/spark-3.5.1-bin-hadoop3"
    export PATH="$SPARK_HOME/bin:$PATH"
    ```

4.  **Verificare la configurazione:** Ricaricare la configurazione della shell (es. `source ~/.bash_profile`) o riavviare il terminale. Verificare che Spark sia accessibile eseguendo:
    ```bash
    spark-shell
    ```
    La visualizzazione del prompt di Scala (`scala>`) conferma il successo dell'installazione. Uscire con `:q`.

### 3. Installazione delle Dipendenze Python

All'interno della directory principale del progetto, installare le librerie Python necessarie tramite Pipenv.
```bash
pipenv install
```

### 4. Download e Preparazione del Dataset

1.  **Scaricare il Dataset:** Il progetto utilizza il dataset "Bitcoin Historical Data" da Kaggle.
    [**Scaricare il dataset da Kaggle**](https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data)

2.  **Posizionare e Rinominare il File:** Dopo il download, posizionare il file `bitstampUSD_1-min_data_2012-01-01_to_2021-03-31.csv` nella directory principale del progetto e rinominarlo in `bitcoin_data.csv`.

3.  **Ordinare i Dati:** Il dataset originale non è ordinato cronologicamente. Eseguire lo script di preparazione per creare una versione ordinata dei dati, necessaria per la corretta funzione del watermark di Spark.
    ```bash
    pipenv run python prepare_data.py
    ```
    Questo comando genera un nuovo file, `bitcoin_data_sorted.csv`, che sarà utilizzato dal simulatore.

_nota:_ I file .CSV non sono stati inclusi nel repository in quanto superavano le dimensioni consentite di 100MB per file.  
Scaricare i dati localmente se si vuole testare il progetto sulla propria macchina seguendo le indicazioni precedenti.  

---

## Esecuzione della Pipeline

Per avviare il progetto, sono necessarie **due finestre di terminale separate**, entrambe posizionate nella directory principale del progetto.

### Terminale 1: Avviare il Simulatore di Stream

Questo script avvia il produttore di dati. Crea una directory `streaming_data/` e inizia a popolarla con piccoli file CSV a intervalli di pochi secondi.
```bash
pipenv run python stream_simulator.py
```
Lasciare questo terminale in esecuzione.

### Terminale 2: Avviare l'Applicazione Spark

Questo comando avvia l'applicazione Spark, che inizia a monitorare la directory `streaming_data/` e a processare i file in arrivo.
```bash
spark-submit spark_streaming_app.py
```

---

## Struttura e Analisi dei Risultati

L'applicazione non stampa i risultati in console, ma li salva in modo persistente su disco in formato Parquet. Al termine dell'esecuzione (interrotta con `Ctrl+C` in entrambi i terminali), la directory del progetto conterrà le seguenti cartelle:

-   **`streaming_data/`**: Contiene i micro-batch di dati generati dal simulatore.
-   **`results_data/`**: Contiene lo storico completo di tutte le aggregazioni calcolate, salvate in formato Parquet.
-   **`results_alerts/`**: Contiene solo i record per i quali è stato generato un alert, salvati in formato Parquet.
-   **`checkpoints_data/` e `checkpoints_alerts/`**: Contengono i metadati di checkpoint di Spark per garantire la fault-tolerance.

### Come Ispezionare i Risultati

Per analizzare i dati salvati, è possibile utilizzare un Python Notebook con la libreria `pandas`.  
Nel progetto viene presentato un esempio di analisi interattiva tramite notebook nel file `analisi.ipynb`