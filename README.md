# New-DBMS-Project
Progetto realizzato per il corso di New Generational Data Model and DBMSS in collaborazione con la mia collega di corso Silvia Colombo

## Link alla pagina github del progetto
https://github.com/Nickbarbieri1/New-DBMS-Project

## Script di generazione del dataset

Il file contenente lo script per la generazione del dataset, _Generate\_dataset.py_, riceve da linea di comando la dimensione del dataset in MB del dataset da generare. Per fare in modo di generare un dataset che soddisfa la dimensione specificata, durante la generazione viene fatto variare il numero di giorni su cui andare a definire le transazioni, in base al numero di iterazioni fallite in precedenza.