iris = None


def write_to_file(file, string):
    f = open(file, "w")
    f.write(string)
    f.close()
       
       
def process(parameters):
    try:
        import numpy as np
        from sklearn import cross_validation
        from sklearn import datasets
        from sklearn import svm
    except ImportError, e:
        subprocess.call(["sudo","easy_install","pip"])
        subprocess.call(["sudo","pip","install","scikit-learn"])
        import numpy as np
        from sklearn import cross_validation
        from sklearn import datasets
        from sklearn import svm
        
    #load dataset only once
    global iris
    if not iris:
        iris = datasets.load_iris()
        
    #run experiment
    clf = svm.SVC(kernel='rbf', C=float(parameters))
    scores = cross_validation.cross_val_score(clf, iris.data, iris.target, cv=5)
    s = "Accuracy: %0.2f (+/- %0.2f)" % (scores.mean(), scores.std() / 2)
    tmp_file = "/tmp/" + parameters + ".txt"
    write_to_file(tmp_file,s)    
    cp("file://" + tmp_file, "s3n://dl-stl-ml-awsdev/accuracies/" + parameters + ".txt")
