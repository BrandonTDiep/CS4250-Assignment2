# -------------------------------------------------------------------------
# AUTHOR: Brandon Diep
# FILENAME: db_connection.py
# SPECIFICATION: Connects to my DB and performs CRUD operations
# FOR: CS 4250- Assignment #2
# TIME SPENT: 6 hours
# -----------------------------------------------------------*/

# IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

# importing some Python libraries
# --> add your Python code here
import psycopg2
import re
from psycopg2.extras import RealDictCursor


def connectDataBase():
    # Create a database connection object using psycopg2
    # --> add your Python code here
    DB_NAME = "corpus"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn

    except:
        print("Database not connected successfully")


def createCategory(cur, catId, catName):
    # Insert a category in the database
    # --> add your Python code here

    sql = "Insert into categories (id_cat, name) VALUES (%s, %s)"

    recset = [catId, catName]
    cur.execute(sql, recset)


def createDocument(cur, docId, docText, docTitle, docDate, docCat):
    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    getCategorySql = "SELECT categories.id_cat FROM categories WHERE categories.name = %(catName)s"
    cur.execute(getCategorySql, {'catName': docCat})
    resultCatId = cur.fetchone()

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    text = re.sub(r'[^\w\s]', "", docText)
    text = text.lower()
    num_char = 0
    for char in (list(text.replace(" ", ""))):
        num_char += 1

    createDocSql = "INSERT INTO documents (doc, text, title, num_chars, date, id_cat) VALUES (%s, %s, %s, %s, %s, %s)"
    recCreateDocSet = [docId, docText, docTitle, num_char, docDate, resultCatId['id_cat']]
    cur.execute(createDocSql, recCreateDocSet)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    # --> add your Python code here
    terms = text.split()
    getTermSql = "SELECT term FROM terms WHERE term = %(term)s"
    for term in terms:
        cur.execute(getTermSql, {'term': term})
        existingTerm = cur.fetchone()

        if not existingTerm:
            insertTermSql = "INSERT INTO terms (term, num_chars) VALUES (%s, %s)"
            recCreateTermSet = [term, len(term)]
            cur.execute(insertTermSql, recCreateTermSet)

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here
    dictTerm = {}
    for term in terms:
        if term in dictTerm:
            dictTerm[term] = dictTerm[term] + 1
        else:
            dictTerm[term] = 1

    for term, termCount in dictTerm.items():
        insertIndexSql = "INSERT INTO index (doc, term, term_count) VALUES (%s, %s, %s)"
        recCreateIndexSet = [docId, term, termCount]
        cur.execute(insertIndexSql, recCreateIndexSet)


def deleteDocument(cur, docId):
    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here
    getIndexSql = "SELECT term FROM index WHERE doc = %(docId)s"
    cur.execute(getIndexSql, {'docId': docId})
    recIndexSet = cur.fetchall()

    deleteIndexSql = "DELETE FROM index WHERE doc = %(docId)s"
    cur.execute(deleteIndexSql, {'docId': docId})

    for rec in recIndexSet:
        checkTermInDocSql = "SELECT doc FROM index WHERE term = %(term)s"
        cur.execute(checkTermInDocSql, {'term': rec['term']})
        existingIndex = cur.fetchone()

        if not existingIndex:
            deleteTermSql = "DELETE FROM terms WHERE term = %(term)s"
            cur.execute(deleteTermSql, {'term': rec['term']})

    # 2 Delete the document from the database
    # --> add your Python code here
    deleteDocSql = "DELETE FROM documents WHERE doc = %(docId)s"
    cur.execute(deleteDocSql, {'docId': docId})


def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    # 1 Delete the document
    # --> add your Python code here
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    # --> add your Python code here
    createDocument(cur, docId, docText, docTitle, docDate, docCat)


def getIndex(cur):
    invertedIndex = {}
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    getInvertedIndexSql = ("SELECT term, title, term_count FROM index JOIN documents ON index.doc "
                           "= documents.doc ORDER BY term ASC")
    cur.execute(getInvertedIndexSql)
    recInvertedIndexSet = cur.fetchall()

    for rec in recInvertedIndexSet:
        if rec['term'] in invertedIndex:
            invertedIndex[rec['term']].update({rec['title']: rec['term_count']})
        else:
            invertedIndex[rec['term']] = ({rec['title']: rec['term_count']})

    return invertedIndex

# I am not sure if we need to a method for create table but here it is
def createTables(cur, conn):
    try:
        sql = "create table categories(id_cat integer not null, name character varying(255) not null, " \
              "constraint categories_pk primary key (id_cat))"
        cur.execute(sql)

        sql = "create table documents(doc integer not null, text character varying(255) not null, " \
              "title character varying(255) not null, num_chars integer not null, date date not null, id_cat integer not null," \
              "constraint documents_pk primary key (doc), " \
              "constraint documents_id_cat_fkey foreign key (id_cat) references categories (id_cat))"
        cur.execute(sql)

        sql = "create table terms(term character varying(255) not null, num_chars integer not null, " \
              "constraint terms_pk primary key (term))"
        cur.execute(sql)

        sql = "create table index(doc integer not null, term character varying(255) not null, term_count integer not null, " \
              "constraint index_pk primary key (term, doc), " \
              "constraint index_doc_fkey foreign key (doc) references documents (doc), " \
              "constraint index_term_fkey foreign key (term) references terms (term))"
        cur.execute(sql)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("There was a problem during the database creation or the database already exists.")
        print("An error occurred:", e)

