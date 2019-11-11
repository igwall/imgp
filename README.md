## IMGP Makes Good Prediction - Click prediction project

### Clone the project : 
```
git clone https://github.com/igwall/imgp.git
cd imgp
```

### Requirement 

- Scala version 2.12.10
- sbt version 1.3.3

### Command to launch and use the project :

#### Trainning process : 

```
$ sbt “run learn”
```

And then write the path to the file you want to use to learn the model when the program asks you.

#### Predict process : 

```
$ sbt “run predict”
```

And then write the path to the file you want to use for your prediction when the program asks you.

