# Analysing Metabilic Networks in Gradoop
Dieses Repository enthält die  Ergebnisse des Big-Data Praktikum SS2017 mit dem Thema "Analysing Metabolic Networks in Gradoop" der Universität Leipzig.

## Inhalt
Das Ziel des Praktikums war das Einlesen eines vorgegebenen BiGG Model Datensatz (siehe Datenquelle) in Gradoop und das Aufzeigen von beispielhaften Analysen und Auswertungen, welche mit Gradoop möglich sind. 
Das Projekt basiert auf [Gradoop](https://github.com/dbs-leipzig/gradoop) und wurde um zwei Pakete "jsonConverter" und "metabolism" erweitert, die im Example-Package zu finden sind.

----

## Software
* Gradoop
* Gradoop-Vis
* Apache Maven
----
## Installation
Das Projekt ist mit Apache Maven entwickelt worden und kann im Ordner Gradoop mit dem Befehl:
```sh
mvn install
```

gebaut werden.

----
## Datenquelle
Die Datenquelle ist auf [bigg.ucsd.edu](http://bigg.ucsd.edu/) zu finden. Dabei handelt es sich nach eigener Aussage um ein "knowledgebase of genome-scale metabolic network reconstructions". Das Einlesen wurde mit dem [Testdatensatz](http://bigg.ucsd.edu/models/iAB_RBC_283) ausgiebig getestet. Andere Datensätze wurden ebenfalls stichprobenartig erfolgreich eingelesen.

----
## Ausführen des Projekts

### jsonConverter:
* die Main-Klasse benötigt zwei Argumente: 
   input path: den Pfad zum BiGG-Model-Datensatz 
    output path: den Pfad zu dem Ordner, wo die konvertierten Daten gespeichert werden sollen
* der Output sind EPGM-Daten (extended property graph model) im JSON-Format
*  Argumente setzen und starten
*  die EPGM-Daten (edges.json, nodes.json, graphs.json) werden in den Ordner (output path) geschrieben
	
### metabolism:
*  die Main-Klasse benötigt als Argument den Pfad zum EPGM
*  die Metabolism-Klasse enthält Methoden zur Analyse des Graphen mittels Gradoop-Operationen
*  die meisten Methoden erzeugen einen neuen Logischen Graphen und schreiben ihn in einen Unterordner
*  existierende Unterordner werden überschrieben
*  in der Main-Klasse werden beispielhaft ein paar Methoden aufgerufen, deren Ergebnisse gespeichert oder in der Konsole ausgegeben werden
```java
        //args of main class
        Metabolism mtb = new Metabolism(args);

		// finds all active transport reactions and write them to file
		mtb.getActiveTransportReactions();

		// prints number of incoming and outgoing edges for each vertex
		mtb.getVertexEdges(0, 0);

		// writes all subsystems to one file
		mtb.getSubsystems();

		// writes all graphs with label "reaction" to a separate file in a
		// subfolder reaction
		mtb.writeSubsystems2File("reaction");

		// groups the graph by subsystems, transform vertex labels and write
		// graph to file
		mtb.grouping();
```
*  die gespeicherten EPGM-Daten können mit Gradoop-Vis visualisiert werden.

