# Analysing Metabilic Networks in Gradoop
Dieses Repository enthält die  Ergebnisse des Big-Data Praktikum SS2017 mit dem Thema "Analyzing Metabolic Networks with Gradoop" der Universität Leipzig.

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
Die Datenquellen sind auf [bigg.ucsd.edu](http://bigg.ucsd.edu/) zu finden. Dabei handelt es sich nach eigener Aussage um: "nowledgebase of genome-scale metabolic network reconstructions.". Unser [Testdatensatz](http://bigg.ucsd.edu/models/iAB_RBC_283) ist dabei nur ein Teildatensatz für den das Einlesen ausgibig getestet wurde. Stichproben mit anderen Datensätzen wurden ebenfalls erfolgreich eingelesen.

----
## Ausführen des Projekts

### jsonConverter:
* die Main-Klasse benötigt zwei Argumente: den Pfad zum BiGG-Model-Datensatz(input path) und den Pfad zu dem Ordner, wo   die konvertierten Daten (EPGM) gespeichert werden sollen (output path)
*  Argumente setzen und starten
*  EPGM Daten (edges.json, nodes.json, graphs.json) werden in den Ordner (output path) geschrieben
	
### metabolism:
*  die Main-Klasse benötigt als Argument den Pfad zum EPGM
*  die Metabolism-Klasse enthält Methoden zur Analyse des Graphen mittels Gradoop-Operationen
*  die meisten Methoden erzeugen einen neuen Logischen Graphen und schreiben ihn in einen Unterordner
*  existierende Unterordner werden überschrieben
*  die frequent-subgraph-mining-Methode ist für unsere Form der Konvertierung nicht anwendbar, da bei uns keine Label gehäuft 		  auftreten.
*  die pattern-matching-Methode funktioniert nur bedingt, z.B. ist es nicht möglich Kreise zu finden
*  die longestPath-Methode findet nur einen langen Pfad, nicht unbedingt den längsten (auch abhängig von der edge-Sortierung)
*  in der Main-Klasse werden beispielhaft ein paar Methoden aufgerufen, deren die Ergebnisse gespeichert oder in der Konsole ausgegeben werden
*  die gespeicherten EPGM-Daten können mit Gradoop-Vis visualisiert werden.

