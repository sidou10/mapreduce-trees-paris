# Basic computations on .csv file using MapReduce

- Question 1: number of trees by type
```
Araucariaceae	1
Betulaceae	4
Bignoniaceae	1
Cannabaceae	1
Cornaceae	1
Cupressaceae	1
Ebenaceae	4
Eucomiaceae	1
Fabaceae	3
Fagaceae	12
...
```
- Question 2: height of the heighest tree by type
```
Araucariaceae	9.0
Betulaceae	20.0
Bignoniaceae	15.0
Cannabaceae	16.0
Cornaceae	12.0
Cupressaceae	20.0
Ebenaceae	14.0
Eucomiaceae	12.0
Fabaceae	11.0
...
```
- Question 3: borough of the oldest tree in paris

Note that here the output contains one row per tree in the input document. The first line of the output answers the question (the oldest tree (it was planted in 1601) is in the 5th borough).
```
1601	5
1772	16
1782	16
1784	12
...
```
