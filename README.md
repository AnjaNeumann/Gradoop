# Analysing Metabilic Networks in Gradoop
Dieses Repository enthält die  Ergebnisse des Modul BigData Praktikum SS2017 der Uni Leippzig.

##Inhalt
Das Ziel des Praktikums war das Einlesen eines vorgegebenen BiGG Model (siehe Datenquelle) in Gradoop und das Aufzeigen von beispielhaften Analysen und Auswertungen, welche mit Gradoop möglich sind. 

Das Project unterteilt sich dabei in die zwei Programme "jsonConverter" und "metabolism". 
----

##Software
* Gradoop
* Gradoop-Vis
* Apache Maven
----
##Installation
Das Projekt ist mit Apache Maven entwickelt worden und kann im Ordner Gradoop mit dem Befehl:
```sh
mvn install
```
gebaut werden.
----
##Datenquelle
Die Datenquellen sind auf [bigg.ucsd.edu](http://bigg.ucsd.edu/) zu finden. Dabei handelt es sich nach eigener Aussage um eine "nowledgebase of genome-scale metabolic network reconstructions.". Unser [Testdatensatz](http://bigg.ucsd.edu/models/iAB_RBC_283) ist dabei nur ein Teildatensatz für den das Einlesen ausgibig getestet wurde. Stichproben mit anderen Datensätzen wurden ebenfalls erfolgreich eingelesen.

----
##Ausführen des Projekts

jsonConverter:
	* die Main-Klasse benötigt zwei Argumente: den Pfad zum BiGG-Model-Datensatz (input path) 
	  und den Pfad zu dem Ordner, wo die konvertierten Daten (EPGM) gespeichert werden sollen (output path)
	*  Argumente setzen und starten
	*  EPGM Daten werden in den Ordner (output path) geschrieben
	
metabolism:
	*  die Main-Klasse benötigt als Argument den Pfad zum EPGM
	*  die Metabolism-Klasse enthält Methoden zur Analyse des Graphen mittels Gradoop-Operationen
	*  die meisten Methoden erzeugen einen neuen Logischen Graphen und schreiben ihn in einen Unterordner
	*  existierende Unterordner werden überschrieben
	*  frequent subgraph mining ist für unsere Form der Konvertierung nicht anwendbar, da bei uns keine Label gehäuft 		  auftreten.
	*  pattern matching funktioniert nur bedingt, z.B. ist es nicht möglich Kreise zu finden
	*  longestPath findet nur einen langen Pfad, nicht unbedingt den längsten (auch abhängig von der edge-Sortierung)

