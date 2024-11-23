import time
import math
import pickle

class Score(object):
    def __init__(self, termLists, postingLists, PRdict, titleIdDict):
        self.__parse_posting(postingLists)
        self.termfq = [l[0] for l in termLists]
        self.termdf = [l[1] for l in termLists]
        self.PRdict = PRdict
        self.documentN = 6091006.0
        self.avgLen = 359.92
        self.titleIdDict = titleIdDict
        
    def __parse_posting(self, postingLists):
        self.docIdList = []
        self.docLenList = []
        self.docTfList = []
        self.docTfNormList = []
        for postingList in postingLists:
            self.docIdList.append([l[0] for l in postingList])
            self.docLenList.append([l[2] for l in postingList])
            self.docTfList.append([l[1] for l in postingList])
            self.docTfNormList.append([l[3] for l in postingList])

    def bm25(self):
        scoreDict = {}
        time1 = time.time()
        k1 = 3.0
        b = 0.75
        for index, tfq in enumerate(self.termfq):
            term_w = (2 * tfq / (1.0 + tfq)) * math.log2((self.documentN - self.termdf[index] + 0.5 )/ (self.termdf[index] + 0.5))
            for idx, docid in enumerate(self.docIdList[index]):
                doc_w = term_w * (k1 + 1) * self.docTfList[index][idx] / (self.docTfList[index][idx] + k1 * (1 - b + b * self.docLenList[index][idx]/self.avgLen))
                if docid in scoreDict:
                    scoreDict[docid] += doc_w
                else:
                    scoreDict[docid] = doc_w
        time2 = time.time()
        print("build dict time...", time2-time1)
        
        # Sort the scores
        sortid = sorted(scoreDict.items(), key=lambda item: item[1], reverse=True)
        
        time3 = time.time() 
        print("sort score time...", time3-time2)
        
        return sortid
        
        # Fetch titles using titleIdDict, handling missing docids
        result_titles = []
        for item in sortid:
            docid = item[0]
            print(titleIdDict[docid])
            if docid in self.titleIdDict:
                result_titles.append(self.titleIdDict[docid])  # Append the title from titleIdDict
            else:
                result_titles.append(f"Unknown Title for docid {docid}")  # Placeholder for missing docid

        return result_titles

if __name__ == '__main__':
    # Example termLists and postingLists
    termLists = [[1, 1000], [1, 2000], [2, 1500]]
    postingLists = [
        [[1, 100, 200, 20], [2, 50, 150, 40], [3, 100, 200, 10], [4, 50, 150, 5], [10, 100, 200, 20]], 
        [[2, 10, 150, 40], [3, 30, 200, 10], [10, 50, 200, 20]], 
        [[3, 80, 200, 10], [10, 80, 200, 20]]
    ]
    PRDict = {1: 0.5, 2: 0.3, 3: 0.7, 4: 0.9, 10: 0.8}
    with open('./text/titleIdDict.pkl', 'rb') as f:
    	titleIdDict = pickle.load(f)
    	
    print("First few entries of titleIdDict:")
    for i, (key, value) in enumerate(titleIdDict.items()):
      if i == 10:  # Print only first 10 entries for inspection
        break
    print(f"Key: {key}, Value: {value}")

    computeScore = Score(termLists, postingLists, PRDict, titleIdDict)
    
    # Print term frequency and other parameters for inspection
    print(computeScore.termfq)
    print(computeScore.docIdList)
    print(computeScore.docLenList)
    print(computeScore.docTfList)
    
    print("===bm25===")
    docid = computeScore.bm25()
    print(docid)  # Will print the titles (or placeholders) corresponding to docids

