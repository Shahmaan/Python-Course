from datahanteringsClass import dataManagement
from plotClass import plotter


def main():
    cov = dataManagement("vaccin_covid.csv", "vaccin_covid.db", "covidVaccin")
    
    cov.sep("vaccines", ",")

    cov.database()

    cov.fill_na_values()
    
    cov.vac_and_source("vaccines_and_source", "covidVaccin")

    cov.total_vac("total_vac", "covidVaccin")
    
    cov.daily_vac("daily_vac", "covidVaccin")

    cov.people_vac("people_vac", "covidVaccin")
    
    balkan = plotter("vaccin_covid.db")
    balkan.plotting()
    
    


if __name__ == "__main__":
    main()