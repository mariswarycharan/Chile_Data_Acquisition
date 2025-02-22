How to run this project:

Step 1: First open Control_File.xlsx from the directory Control/Control_File.xlsx.

Step 2: Inputs to be done by the user

->In the Second Row First Cell Needs input as Year-Month(Example:2024-1) ->In the
Second Row Second Cell Fill the corresponding month and year sheet link for
cenabast from the following drive link:
https://drive.google.com/drive/folders/1n6RlnUCTwVSI6NBtp83K1VNYRpw1P4-f

Make sure the input is in correct format and the sheet link provided is also correct for the
corresponding month and year.

Step 3: open Chile.py from the directory Code/Chile.py

Step 4: Run Chile.py file by using this command in the terminal

->  py  .\Code\Chile.py

Step 5: This Code runs for 5-6 hours to give output.

After Code Run Completion the output will be generated in the Output Folder created by the
code. There will be three outputs

-> Pharmatender Format(Only Mercado Publico)

-> Chile Combined(Mercado Publico + Cenabast)

-> Combinacion Vertical(For Dashboard Building)

Important: The Output provided in the folder Chile Combined and Combinacion Vertical
contains Cenabast duplicates in MP Origin. So, while building dashboard make sure in
Combinacion Vertical File, Column:Intitución Destinataria Homologada - Doesn’t Contain
Cenabast.

