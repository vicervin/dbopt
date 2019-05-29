
import psycopg2
import sys
import time

from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.model_selection import GridSearchCV

class MeanClassifier(BaseEstimator, ClassifierMixin):  
    """An example of classifier"""

    def __init__(self, intValue=0, stringParam="defaultValue", otherParam=None):
        """
        Called when initializing the classifier
        """
        self.intValue = intValue
        self.stringParam = stringParam

        # THIS IS WRONG! Parameters should have same name as attributes
        self.differentParam = otherParam 


    def fit(self, X, y=None):
        """
        This should fit classifier. All the "work" should be done here.

        Note: assert is not a good choice here and you should rather
        use try/except blog with exceptions. This is just for short syntax.
        """
        def runQueries(timeout):
            server = 'localhost'
            database = 'tpch'
            username = 'postgres'
            password = 'postgres'
            
            #queries = [14,2,9,#20,6,#17,#18,8,#21,13,3,22,16,4,11,15,1,10,19,5,7,12]
            queries = [14,2,9,6,8,13,3,22,16,4,11,15,1,10,19,5,7,12]
            times = []
            count = 0
            for query in queries:
                    try:
                            cnxn = psycopg2.connect(f'host={server} dbname={database} user={username} password={password}')
                            
                            start = time.time()
                            cursor = cnxn.cursor()
                            cursor.execute(f"SET statement_timeout = '{timeout}s'")
                            cursor.execute(open(f"queries/{query}.sql", "r").read())
                            times.append(time.time() - start)
                            #(number_of_rows,)=cursor.fetchone()
                            print(str(count+1) + " : Query "+ str(queries[count]) + "| time : "+ str(time.time() - start) )#+ "| rows:" + str(number_of_rows))
                            cursor.close()
                    except Exception as e:
                            print(str(count+1) + " : Query "+ str(queries[count]) +" : "+ str(e))
                    count += 1
            #print("Total Query time : "+ str(sum(times)))
            return sum(times)

        assert (type(self.intValue) == int), "intValue parameter must be integer"
        assert (type(self.stringParam) == str), "stringValue parameter must be string"
        #assert (len(X) == 20), "X must be list with numerical values."

        #self.treshold_ = (sum(X)/len(X)) + self.intValue  # mean + intValue
        self.treshold_ = runQueries(self.intValue)

        return self

    def _meaning(self, x):
        # returns True/False according to fitted classifier
        # notice underscore on the beginning
        return( True if x >= self.treshold_ else False )

    def predict(self, X, y=None):
        try:
            getattr(self, "treshold_")
        except AttributeError:
            raise RuntimeError("You must train classifer before predicting data!")

        return([self._meaning(x) for x in X])

    def score(self, X, y=None):
        # counts number of values bigger than mean
        #return(sum(self.predict(X))) 
        return self.treshold_

X_train = [i for i in range(0, 100, 5)]  
X_test = [i + 3 for i in range(-5, 95, 5)]  
tuned_params = {"intValue" : [2,5,10,15]}
#print(len(X_test),len(X_train))
gs = GridSearchCV(MeanClassifier(), tuned_params)

# for some reason I have to pass y with same shape
# otherwise gridsearch throws an error. Not sure why.
gs.fit(X_train, y=[1 for i in range(20)])

print(gs.best_params_) # {'intValue': -10} # and that is what we expect :)  
