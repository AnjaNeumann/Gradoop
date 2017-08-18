# Gradoop
# gradoop-metabolism-example

jsonConverter:
	- die Main-Klasse benötigt zwei Argumente: den Pfad zum BiGG-Model-Datensatz (input path) 
	  und den Pfad zu dem Ordner, wo die konvertierten Daten (EPGM) gespeichert werden sollen (output path)
	- Argumente setzen und starten
	- EPGM Daten werden in den Ordner (output path) geschrieben
	
metabolism:
	- die Main-Klasse benötigt als Argument den Pfad zum EPGM
	- die Metabolism-Klasse enthält Methoden zur Analyse des Graphen mittels Gradoop-Operationen
	- die meisten Methoden erzeugen einen neuen Logischen Graphen und schreiben ihn in einen Unterordner
	- existierende Unterordner werden überschrieben
	- frequent subgraph mining ist für unsere Form der Konvertierung nicht anwendbar, da bei uns keine Label gehäuft 		  auftreten.
	- pattern matching funktioniert nur bedingt, z.B. ist es nicht möglich Kreise zu finden
