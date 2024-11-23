import pickle, json
import sqlite3
import sys, os, time, operator
import math

def tfNorm():
    docDict = {}
    for char in charlist:
        print("Processing...", char)
        for file in files:
            filename = f'./text/{char}/{file[-2:]}'
            if os.path.exists(filename):
                words_dict = pickle.load(open(filename, 'rb'))
                for term, doc_dict in words_dict.items():
                    for doc, num in doc_dict.items():
                        if doc in docDict:
                            docDict[doc] += math.pow(1 + math.log10(num[0]), 2)
                        else:
                            docDict[doc] = math.pow(1 + math.log10(num[0]), 2)
    return docDict

def mergeDict(oriDict, subDict, docDict):
    for term, dict in subDict.items():
        if term in oriDict:
            oriDict[term] += [(int(doc), num[0], num[1], math.sqrt(docDict[doc])) for doc, num in dict.items()]
        else:
            oriDict[term] = [(int(doc), num[0], num[1], math.sqrt(docDict[doc])) for doc, num in dict.items()]
    return oriDict

def writePosting(char, oriDict, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        for term, posting in oriDict.items():
            sorted_posting = sorted(posting, key=operator.itemgetter(1), reverse=True)
            encode_posting = json.dumps(sorted_posting)
            pos = f.tell()
            f.write(encode_posting + '\n')
            t = (term, encode_posting)
            c.execute("INSERT INTO posting VALUES (?, ?)", t)
            wholeDict[char][term] = (len(sorted_posting), pos)

def loadFile(filename):
    if os.path.exists(filename):
        return pickle.load(open(filename, 'rb'))
    else:
        return {}

def writeFile(dict, filename):
    pickle.dump(dict, open(filename, 'wb'))

def dictInit(list):
    words_dict = {}
    for char in list:
        words_dict[char] = {}
    return words_dict

if __name__ == '__main__':
    charlist = [chr(ord('a') + i) for i in range(26)] + ['number']
    print("Index Charlist...", charlist)

    db_path = 'wsm.db'
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''DROP TABLE IF EXISTS posting''')
    c.execute('''CREATE TABLE posting (term TEXT PRIMARY KEY, postings TEXT)''')

    folder = './text/AA/'
    files = os.listdir(folder)
    wholeDict = dictInit(charlist)
    print("Compute tfNorm")
    docDict = tfNorm()

   

    for char in charlist:
        print("Processing...", char)
        start_time = time.time()
        oriDict = {}
        for file in files:
            filename = f'./text/{char}/{file[-2:]}'
            if os.path.exists(filename):
                subDict = loadFile(filename)
                oriDict = mergeDict(oriDict, subDict, docDict)
        print("Counting...", len(oriDict))
        print("Writing posting...")
        # Ensure directory exists before writing the posting file
        os.makedirs(f'./text/{char}', exist_ok=True)
        writePosting(char, oriDict, f'./text/{char}/posting')
        print("Time taken...", time.time() - start_time)
    
    print("Total terms...", sum([len(wholeDict[i]) for i in charlist]))
    conn.commit()
    conn.close()

    # Save term dict to file
    writeFile(wholeDict, './text/termDict')

