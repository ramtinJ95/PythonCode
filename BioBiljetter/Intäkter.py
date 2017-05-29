class Biograf:
    """ En klass som representerar våra biografers namn samt ålder på besökare och pris samt beläggning i programmet"""

    def __init__(self, namn, vuxen_pris, pensionär, barn, antal_platser):
        """Instansieringsmetoden definerar vi objektets attribut och ger dem värden"""

        self.namn = namn
        self.vuxen_pris = vuxen_pris
        self.pensionär = pensionär
        self.barn = barn
        self.antal_platser = antal_platser

    def __str__(self):
        """En specialmetod som anropas varje gång man försöker skriva ut objektet med print, ger en sträng som innehåller priserna och platser"""

        return "Namn: " + self.namn.ljust(20) \
               + "Vuxen Pris: " + str(self.vuxen_pris).ljust(8) \
               + "Pensionär Pris: " + self.pensionär.ljust(8) \
               + "Barn Pris: " + self.barn.ljust(8) \
               + "Antalet platser: " + self.antal_platser \

def intro():

    """Som man kan se på namnet är detta en funktion som presenterar den information
    som användaren kommer behöva för att kunna använda programmet."""

    print("INTRO")
    print()
    print('''Detta program ger dig som ägare av biografer
möjligheten att beräkna inkomsten av varje
biograf samt hur många procent av den totala
mängden platser som sålts.''')
    print()
    print('''När programmet startar så kommer de biografer
som redan finns skrivas ut samt att du som
användare kommer frågas om ett val. Dessa val
är:''')
    print()
    print(''' 1) om du skriver in siffran 1 så kommer du
kunna lägga till en biograf''')
    print()
    print(''' 2) om du skriver in siffran 2 så kommer du
ha möjligheten att välja en biograf och skriva
in antalet sålda biljetter. Programmet kommer
då att skriva ut intäkten för den biografen samt
beläggningen för just den biografen''')
    print()
    print('''3) om du skriver in siffran 3 så kommer programmet att
avslutas. När programmet avslutas så kommer den skriva ut
en lista där biograferna sorteras efter beläggningsgrad,
dessutom så kommer den totala intäkten från alla
biografer skrivas ut.''')


def read_cinemas(data_file):
    """Den här funktionen används för att läsa in befintlig information om varje biograf som redan finns på filen
    den skriver också ut hur många biografer som den hittade."""

    summery_of_info = []
    divider = 5  # detta är bara en godtycklig variabel eftersom efter 5 rader så är det slut på info om en godtycklig biograf
    file = open(data_file, "r")
    row = file.readline()
    while row != "":  # om raden inte är tom gör en temporär lista
        temp_list = []
        for i in range(divider):  # går igenom data filen och läser in varje rad
            temp_list.append(row)
            row = file.readline()

        namn = str(temp_list[0].strip())  # Här tilldelas varje biograf objekt sina attribut genom den temporära listan
        vuxen_pris = temp_list[1].strip()
        pensionär = temp_list[2].strip()
        barn = temp_list[3].strip()
        antal_platser = temp_list[4].strip()
        biograf_info = Biograf(namn, vuxen_pris, pensionär, barn, antal_platser)  # Skapar objektet
        summery_of_info.append(biograf_info)  # Lagrar nu objekter i listan
    return summery_of_info


def add_new_cinema(summery_of_info):
    """Den här funktionen används då man vill lägga till en biograf, men den läggs inte till i filen utan finns enbart
    i datorns RAM-minne"""

    namn = input("Namnge biografen: ")
    vuxen_pris = input("Sätt priset för en vuxenbiljet i heltal: ")
    pensionär = input("Sätt priset för en pensionärbiljet i heltal: ")
    barn = input("Sätt priset för en barnbiljet i heltal: ")
    antal_platser = input("Ange antalet platser i biografen i heltal: ")
    biograf_info = Biograf(namn, vuxen_pris, pensionär, barn, antal_platser)
    summery_of_info.append(biograf_info)


def add_new_cinema_to_file(summery_of_info, data_file):
    """Den här funktionen skriver över all information som finns om varje biograf till en fil så att de finns kvar
    nästa gång man startar programmet."""

    file = open(data_file, "w")
    for cinema in summery_of_info:
        namn = cinema.namn
        vuxen_pris = str(cinema.vuxen_pris)
        pensionär = str(cinema.pensionär)
        barn = str(cinema.barn)
        antal_platser = str(cinema.antal_platser)
        file.write(namn + "\n" + vuxen_pris + "\n" + pensionär + "\n" + barn + "\n" + antal_platser + "\n")
    file.close()


def show_all_cinemas(summery_of_info):
    """Den här funktionen skriver helt enkelt ut varje biograf och all information associerad med den."""

    print("Dessa biografer finns")
    for cinema in summery_of_info:
        print(cinema)
    print("\nHittade " + str(len(summery_of_info)) + " biografer")


def sold_tickets(summery_of_info):
    """Den här funktionen tar emot information från användaren angående hur många biljetter som sålts. Den returnerar
    hur mycket man har tjänat, beläggning och namnet på den biograf som användaren sökt efter."""

    cinema_being_checked = input("Ange namnet på den biograf vars intäkter skall beräknas: ")
    sold_vuxen = int(input("Ange antalet sålda vuxenbiljetter: "))
    sold_pensionär = int(input("Ange antalet sålda pensionärsbiljetter: "))
    sold_barn = int(input("Ange antalet sålda barnbiljetter: "))

    for cinema in summery_of_info:
        if cinema.namn.lower() == cinema_being_checked.lower():
            total_revenue = (sold_vuxen * int(cinema.vuxen_pris)) + (sold_pensionär * int(cinema.pensionär)) + (
            sold_barn * int(cinema.barn))
            amount_of_tickets_sold = sold_vuxen + sold_pensionär + sold_barn  # Den här räknar bara ut antalet biljetter, den ovan räknar intäkt
            if amount_of_tickets_sold > int(
                    cinema.antal_platser):  # kollar så att användaren inte överstiger max antalet platser
                print("ogiltigt mängd sålda biljetter")
                break
            procent_of_tickets_sold = (amount_of_tickets_sold / int(cinema.antal_platser)) * 100  # beräknar beläggning

    print("Intäkterna för biografen: " + str(total_revenue))
    print("beläggning för biografen: " + str(procent_of_tickets_sold) + "%")

    return procent_of_tickets_sold, total_revenue, cinema_being_checked


def main():
    rand_map = {}
    all_revenue = 0
    data_file = "biografer.txt"
    summery_of_info = read_cinemas(data_file)
    print()
    intro()
    print()
    show_all_cinemas(summery_of_info)
    print()

    a = True
    while a:
        choice_variable = input("Ange ditt val: ")

        if choice_variable == "1":
            add_new_cinema(summery_of_info)
            add_new_cinema_to_file(summery_of_info, data_file)
        elif choice_variable == "2":
            procentandel, revenue, cinema_being_checked = sold_tickets(summery_of_info)
            all_revenue += revenue  # lägger till intäkter för varje biograf, all_revenue är typ som en global variabel
            key = procentandel  # nyckeln till biografen för dictionaryn
            value = cinema_being_checked
            rand_map[key] = value  # här tilldelas nyckel och värde paren i dictionaryn
        elif choice_variable == "3":
            a = False
    print(
        "Följande är en lista över alla biografer och deras beläggning i procent, sorterade från den högst beläggning först")
    print()
    for key in sorted(rand_map, reverse=True):  # sorterar dictonaryn i den ordning som krävdes.
        print(str(rand_map[key]) + ": " + str(key) + " %")
    print()
    print("Den totala intäkten från alla biografer är: " + str(all_revenue) + "kr")
    input("Press ENTER too Exit")


main()

















