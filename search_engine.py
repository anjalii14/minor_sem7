import json, pickle
import sys, os, re, time, nltk
from nltk.stem.wordnet import WordNetLemmatizer
import mmap
import logging
from score import Score
import spacy
import sqlite3
import ujson

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class searchEngine():
    def __init__(self):
        logging.debug("Initializing the search engine")
        try:
            # Load preprocessed data files
            self.termDict = pickle.load(open('./text/termDict', 'rb'))
            logging.debug("Loaded termDict successfully")
        except FileNotFoundError:
            logging.error("termDict file not found!")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Error loading termDict: {e}")
            sys.exit(1)

        self.charlist = [chr(ord('a') + i) for i in range(26)] + ['number']
        self.lmtzr = WordNetLemmatizer()
        
        try:
            with open('stoplist.txt', 'r') as f:
                stopwords = eval(f.read())
            self.stopwords = stopwords
            logging.debug("Loaded stoplist successfully")
        except FileNotFoundError:
            logging.error("stoplist.txt file not found!")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Error loading stoplist: {e}")
            sys.exit(1)
        
        self.lemmaQuery = []
        self.query = ""
        self.queryDict = self.__dictInit()
        self.nlp = spacy.load('en_core_web_sm')
        
        try:
            self.PRDict = pickle.load(open('PRDict', 'rb'))  # PageRank data
            logging.debug("Loaded PRDict successfully")
        except FileNotFoundError:
            logging.error("PRDict file not found!")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Error loading PRDict: {e}")
            sys.exit(1)

        try:
            self.titleIdDict = pickle.load(open('./text/titleIdDict.pkl', 'rb'))  # Load
            print("Loaded titleIdDict:", self.titleIdDict)
            logging.debug("Loaded titleIdDict successfully")
		
        except FileNotFoundError:
            logging.error("titleIdDict file not found!")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Error loading titleIdDict: {e}")
            sys.exit(1)
        
    def __clean_page(self, text_page):
        logging.debug("Cleaning page text")
        pat_letter = re.compile(r'[^a-zA-Z0-9 \']+')
        pat_is = re.compile("(it|he|she|that|this|there|here)(\'s)", re.I)
        pat_s = re.compile("(?<=[a-zA-Z])\'s") # 's
        pat_s2 = re.compile("(?<=s)\'s?")
        pat_not = re.compile("(?<=[a-zA-Z])n\'t") # not
        pat_would = re.compile("(?<=[a-zA-Z])\'d") # would
        pat_will = re.compile("(?<=[a-zA-Z])\'ll") # will
        pat_am = re.compile("(?<=[I|i])\'m") # am
        pat_are = re.compile("(?<=[a-zA-Z])\'re") # are
        pat_ve = re.compile("(?<=[a-zA-Z])\'ve") # have

        new_text = pat_letter.sub(' ', text_page).strip().lower()
        new_text = pat_is.sub(r"\1 is", new_text)
        new_text = pat_s.sub("", new_text)
        new_text = pat_s2.sub("", new_text)
        new_text = pat_not.sub(" not", new_text)
        new_text = pat_would.sub(" would", new_text)
        new_text = pat_will.sub(" will", new_text)
        new_text = pat_am.sub(" am", new_text)
        new_text = pat_are.sub(" are", new_text)
        new_text = pat_ve.sub(" have", new_text)
        new_text = new_text.replace('\'', ' ')
        return new_text

    # Map POS tags to WordNet tags
    def __get_wordnet_pos(self, treebank_tag):
        if treebank_tag.startswith('J'):
            return nltk.corpus.wordnet.ADJ
        elif treebank_tag.startswith('V'):
            return nltk.corpus.wordnet.VERB
        elif treebank_tag.startswith('N'):
            return nltk.corpus.wordnet.NOUN
        elif treebank_tag.startswith('R'):
            return nltk.corpus.wordnet.ADV
        else:
            return ''

    def __dictInit(self):
        queryDict = {}
        for char in self.charlist:
            queryDict[char] = {}
        return queryDict

    def lemma(self, word):
        if word and word not in self.stopwords:
            tag = nltk.pos_tag(word) # tag is like [('bigger', 'JJR')]
            pos = self.__get_wordnet_pos(tag[0][1])
            if pos:
                lemmatized_word = self.lmtzr.lemmatize(word, pos)
                return lemmatized_word
            else:
                return word

    def wordLemma(self, words):
        doc = self.nlp(words)
        new_words = [token.lemma_ for token in doc]
        return new_words

    def pageLemma(self, page):
        cleanpage = self.__clean_page(page).split()
        lemmawords = self.wordLemma(cleanpage)
        result = []
        for word in lemmawords:
            if word:
                result.append(word)
        return result

    def parseQuery(self):
        logging.debug("Parsing query")
        cleanquery = self.__clean_page(self.query)
        lemmawords = self.wordLemma(cleanquery)
        self.lemmaQuery = []
        self.queryDict = self.__dictInit()
        for word in lemmawords:
            if word and word not in self.stopwords:
                self.lemmaQuery.append(word)
        for lemmaq in self.lemmaQuery:
            first_ch = lemmaq[0]
            if first_ch not in self.charlist[:-1]:
                first_ch = 'number'
            if lemmaq in self.queryDict[first_ch]:
                self.queryDict[first_ch][lemmaq] += 1
            else:
                self.queryDict[first_ch][lemmaq] = 1
        logging.debug(f"Query dict after parsing: {self.queryDict}")

    def __memory_map(self, filename, access=mmap.ACCESS_WRITE):
        size = os.path.getsize(filename)
        fd = os.open(filename, os.O_RDWR)
        return fd, mmap.mmap(fd, size, access=access)

    def loadPostingDb(self):
        logging.debug("Loading posting database")
        db_path = 'wsm.db'
        try:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            postingLists = []
            termLists = []
            for first_ch, termsDict in self.queryDict.items():
                if len(termsDict) > 0:
                    for term, fq in termsDict.items():
                        if term in self.termDict[first_ch]:
                            (df, pos) = self.termDict[first_ch][term]
                            termLists.append([fq, df])
                            cursor = c.execute(f"SELECT postings FROM posting WHERE term=\"{term}\";")
                            for r in cursor:
                                posting = ujson.loads(r[0])
                                postingLists.append(posting[:len(posting)//3])
                        else:
                            logging.info(f"In query: {self.query}, term {term} not in termDict.")
            conn.close()
            logging.debug("Posting database loaded successfully")
            return termLists, postingLists
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")
            return [], []

    def buildQuery(self, query):
        self.query = query
        self.parseQuery()
        termLists, postingLists = self.loadPostingDb()
        self.computeScore = Score(termLists, postingLists, self.PRDict, self.titleIdDict)
    
    def searchByMethod(self, method):
        logging.debug(f"Searching using method: {method}")
        if method == "bm25":
            return self.computeScore.bm25()
        if method == "optimised_tfidf":
            return self.computeScore.optimised_tfidf()  # Using optimized BM25

if __name__ == '__main__':
    start = time.time()
    engine = searchEngine()
    logging.debug("Init time: %s", time.time()-start)
    
    # Take user input for the query
    query = input("Enter your search query: ").strip()
    
    start = time.time()
    engine.buildQuery(query)
    logging.debug("buildQuery time: %s", time.time()-start)
    
    try:
        result = engine.computeScore.bm25()
        logging.debug(f"Search result: {result}")
    except AttributeError as e:
        logging.error(f"Error during search: {e}")

