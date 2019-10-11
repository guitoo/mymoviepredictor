 """The Movie Predictor
Guillaume Meurisse g.meurisse.ia@gmail.com """

import sys
import argparse
import csv
import mysql.connector
from mysql.connector.constants import SQLMode

parser = argparse.ArgumentParser()
parser.add_argument('context', choices=['people','movies'], help="group you want to control")
action_subparsers = parser.add_subparsers(title='action',dest='action',help="action to perform on the selected group",required=True )
list_parser = action_subparsers.add_parser('list',help="request the list of all the element of the group")
find_parser = action_subparsers.add_parser('find',help="search for a specific element by its id")
find_parser.add_argument('id', metavar='id', type=int)

for sub_command in [find_parser,list_parser]:
    sub_command.add_argument('--export', metavar='file.csv',help="export the result of the search in a csv file")
        
args = parser.parse_args()

def connectToDB():
    return mysql.connector.connect(user='predictor', password='predictor',
                                     host='127.0.0.1',
                                     database='predictor')
def disconnectDB(cnx):
    cnx.close()

def createCursor(cnx):
    return cnx.cursor(named_tuple=True)

def closeCursor(cursor):
    cursor.close()

def findQuery(table,id):
    return ("SELECT * FROM {} WHERE id = {}".format(table,id))

def findAllQuery(table):
    return ("SELECT * FROM {}".format(table))

def sendQuery(query):
    cnx = connectToDB()
    cursor= createCursor(cnx)
    cursor.execute(query)
    result = cursor.fetchall()
    closeCursor(cursor)
    disconnectDB(cnx)
    return result

def find(table, id=None):
    if id:
        query=findQuery(table, id)
    else:
        query=findAllQuery(table)
        
    return sendQuery(query)

if args.context == "people":
    if args.action == "find":
        peopleId = args.id      
        result = find("people",peopleId)
        for (person) in result:
            print("#{}: {} {} ".format(person.id, person.firstname, person.lastname))
    elif args.action == "list":
        result = find("people")
        if args.export:
            print("export csv in file {}".format(args.export))
            with open(args.export, 'w',newline='\n',encoding='utf-8') as csvfile:
                writer=csv.writer(csvfile)
                writer.writerow(['person','firstname','lastname'])
                for (person) in result:
                    writer.writerow( [person.id, person.firstname, person.lastname] )
        for (person) in result:
            print("#{}: {} {} ".format(person.id, person.firstname, person.lastname))
#    elif arguments[1] == 'insert'
        
elif args.context == "movies":
    if args.action == "find":
        movieId = args.id
        result = find("movie",movieId)
        for (movie) in result:
            print("#{}: {} {}h{}".format(movie.id, movie.title, movie.duration//60, movie.duration%60))
            #print("#{}: {}".format(movie['id'], movie['title'])





#cursor = cnx.cursor()

#cnx.sql_mode = [ SQLMode.NO_ZERO_DATE, SQLMode.REAL_AS_FLOAT]

#for (id, title, release_date) in cursor:
#    print("({}) {} was released on {}"
#    .format(id, title, release_date))

#query = ("INSERT INTO people "
#        "(firstname, lastname) "
#        "VALUES "
#        "('Billy', 'Zane'),"
#        "('Kathy', 'Bates'), "
#       "('Frances', 'Fisher'); ")

#cursor.execute(query)

#cnx.commit()

#cursor.close()
#cnx.close()