import papermill as pm
import os 

def main(input_notebook, output_notebook, municipality, district_numbers, data_path):
   for number in district_numbers:

      print(f"Running.. district number: {number}")
      # run notebook
      pm.execute_notebook(
         input_path = input_notebook,
         output_path = output_notebook,
         parameters=dict(
            municipality = municipality,
            district_number=number,
            data_path = data_path
            ))
      print(f"Finished running district number: {number}")
   
   print("Finished running {municipality}")

if __name__ == '__main__':
   
   # params
   municipality = "oslo"
   district_numbers = range(30101, 30161)
   data_path = r"/home/NINA.NO/willeke.acampo/Mounts/P-Prosjekter2/152022_itree_eco_ifront_synliggjore_trars_rolle_i_okosyst/TEMP"
   
   
   # path current wd
   project_dir = os.getcwd()
   print(project_dir)
   
   input_notebook = os.path.join(project_dir, "temp", "tree_visibility", "notebooks", "04a_oslo_treeVis_stat_per_district" + ".ipynb")
   print(input_notebook)
   output_notebook = input_notebook # overwrite cells in input notebook
   main(input_notebook, output_notebook, municipality, district_numbers, data_path)