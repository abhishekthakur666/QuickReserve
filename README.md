# QuickReserve

* PYTHON ASYNCIO Based - Fast and efficient car reservation system with great User Experience for manager and customer roles.
* Main features
    - Operator Management to create Manager and Customer User
    - RBAC for different type of users
    - Register Car / Modify Car details / Unregister car
    - Reserved available cars
    - DB Worker pool for concurrent DB Access
    - Unique and non-unique index support for faster db access 
    
   
  * Target OS - Windows 10  
  * How to install and run 
    - Ensure Python3.9+ is installed
    - Ensure pyreadline, prettytable and termcolor libraries are installed (please refer requirement.txt for same)
    - Clone the code
    - python reservecli.py 

* Output (By default Master CLI prompt will be launched)
    - Master admin can create customer operator
        - CMD -  master:abhishek@qr.com#register operators email_address=ravi@qr.com role=customer
    - Master admin can set customer operator password
        - CMD - register op-credentials email_address=ravi@qr.com password=test1234
    - Master admin can create manager operator
        - CMD - register operators email_address=sagar@qr.com role=manager
    - Master admin can set manager operator password
        - CMD - register op-credentials email_address=sagar@qr.com password=test1234
    - Login as Manager
        - CMD - login email_address=sagar@qr.com password=test1234
    - As Manager register new car 
        - CMD - register cars model_name=Tesla reg_no=12345
     - Login as Customer
        - CMD - login email_address=ravi@qr.com password=test1234
     - As customer Show cars by model (Applicable for both manager and customer)
        - CMD - show cars model_name=Tesla
    - As customer Reserve car
        - CMD - register car-reservations reg_no=12345
    - Inspect car reservations (Applicable for both manager and customer)
        - CMD - query car-reservations model_name=Tesla
      


