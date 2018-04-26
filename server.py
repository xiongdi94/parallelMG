import socket
import sys
import time 
s = socket.socket()
s.bind(("localhost",9999))
s.listen(10) # Acepta hasta 10 conexiones entrantes.

while(True):

    sc, address = s.accept()
    if(sc):

        print(address)
        i=1
        f = open ("./input/adj_noun_pairs.txt", "rb")
        l = f.readline()


            # Recibimos y escribimos en el fichero
            #sc.send(l)
        while (l):   
            sc.send(l)
            l = f.readline()
            # time.sleep(0.001)  
            # f.seek(0)
        f.close()
        sc.close()


s.close()
