# -*- coding: utf-8 -*-
import arcpy
import traceback
import pandas as pd
import numpy as np

class Toolbox(object):
    def __init__(self):
        self.label = "EIA summarizer"
        self.alias = "eia_summarizer"
        self.tools = [ProcessTableToEIA]
        self.eia_df = None
        self.desc_df = None

class ProcessTableToEIA(object):
    def __init__(self):
        self.label = "Process EIA"
        self.description = "Runs EIA summary on S123 results and outputs a new table"
        self.canRunInBackground = False


    def getParameterInfo(self):
        params = []

        # 0 - Input table or feature class
        p0 = arcpy.Parameter(
            displayName="Input Feature Class or Table",
            name="in_table",
            datatype=["GPTableView", "GPFeatureLayer"],
            parameterType="Required",
            direction="Input"
        )

        # 1 - Output table
        p1 = arcpy.Parameter(
            displayName="Output Table",
            name="out_table",
            datatype="DETable",
            parameterType="Required",
            direction="Output"
        )

        # 2 - Optional Excel output
        p2 = arcpy.Parameter(
            displayName="Optional Excel Output (.xlsx)",
            name="out_excel",
            datatype="DEFile",
            parameterType="Optional",
            direction="Output"
        )

        # Restrict to .xlsx
        p2.filter.list = ["xlsx"]

        params = [p0, p1, p2]
        return params

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return
    
    def loadInputTable(self, parameters):
        try:
            in_table = parameters[0].valueAsText
            arcpy.AddMessage("Reading input table...")

            # Convert to pandas DataFrame (fast & flexible)
            

            # load fields in input table
            bad_types = ("Geometry", "OID", "Blob", "Raster", "Guid", "Date")

            fields = [f.name for f in arcpy.ListFields(in_table)
                    if f.type not in bad_types]

            arcpy.AddMessage(f"Using {len(fields)} fields")

            rows = []
            with arcpy.da.SearchCursor(in_table, fields) as cursor:
                for row in cursor:
                    rows.append(row)

            import pandas as pd
            self.eia_df = pd.DataFrame(rows, columns=fields)

            arcpy.AddMessage(f"Loaded {len(self.eia_df)} records.")

        except Exception as e:
            tb = traceback.format_exc()
            arcpy.AddError("Tool failed!")
            arcpy.AddError(tb)
            raise
    
    def loadEIADefinitions(self):
            # Read in EIA mapping dictionaries from external CSV
            input_dicts_path = "N:/Research/CNHP/SharedStorage/Clark/PyTools/EIA_DescriptionDictionary_edits.csv"
            self.desc_df = pd.read_csv(input_dicts_path)

    def summarizeEIA(self):
                    # convert text to number
            mapping = {
                "A": 5,
                "B": 4,
                "C": 3,
                "C-": 2,
                "D": 1,
                "": pd.NA
            }

            def convert_to_score(s):
                s = s.astype(str).str.strip()
                out = (
                    s.str.startswith("C-").map({True: 2, False: pd.NA})
                    .fillna(s.str[0].map(mapping))
                )
                # Return nullable integer column
                return out.astype("Int64")

            eia_cols = ['L1', 'L2', 'B1', 'B2', 'B3a', 'B3b',
                                'V1', 'V2', 'V3', 'V4herb', 'V4woody', 'V5', 'V6',
                                'H1_metrics', 'H2_Hydroperiod', 'H3Marsh', 'H3Playa', 'H3Riverine','S1_substrate', 'S2_surfacewater', 'S3_algalgrowth', 'size_rating']
            self.eia_df[eia_cols] = self.eia_df[eia_cols].apply(convert_to_score)

            # compress h3_columns
            h3_cols = ['H3Playa', 'H3Riverine', 'S1_substrate']
            self.eia_df["H3"] = self.eia_df[h3_cols].bfill(axis=1).iloc[:, 0]

            # compress V4_columns
            v4_cols = ['V4herb', 'V4woody']
            self.eia_df["V4"] = self.eia_df[v4_cols].bfill(axis=1).iloc[:, 0]

            eia_cols_req = ['L1', 'L2', 'B1', 'B2', 'B3a', 'B3b',
                                'V1', 'V2', 'V3', 'V4',
                                'H1_metrics', 'H2_Hydroperiod', 'H3','S1_substrate']

            # Calculate Condtion, Landscape, EIA Scores and Ranks
            def compute_scores(eia_row):
                # validate required data
                for col in eia_cols_req:
                    if pd.isna(eia_row[col]):
                        return pd.Series([pd.NA, pd.NA, pd.NA, pd.NA],
                                        index=["land_score", "condition_score", "eia_score", "eo_score"])
                    
                ########### Calculate landscape score
                l = (eia_row['L1'] + eia_row['L2'])/2
                b = np.sqrt(np.sqrt((eia_row['B1'] * eia_row['B2'])*((eia_row['B3a'] + eia_row['B3b'])/2)))
                land_score = l * 0.33 + b * 0.67

                ####### calculate condition score
                # handle woody vs herb
                if pd.isna(eia_row['V5']) or pd.isna(eia_row['V6']):
                    v = (eia_row['V1'] + eia_row['V2'] + eia_row['V3'] + eia_row['V4'])/4
                else:
                    v = (eia_row['V1'] + eia_row['V2'] + eia_row['V3'] + eia_row['V4'] + eia_row['V5'] + eia_row['V6'])/6

                h = (eia_row['H1_metrics'] + eia_row['H2_Hydroperiod'] + eia_row['H3'])/3

                # handle water vs no water vs no algae
                if pd.isna(eia_row['S3_algalgrowth']):
                    p = eia_row['S1_substrate']
                elif pd.isna(eia_row['S2_surfacewater']):
                    p = (eia_row['S1_substrate'] + eia_row['S3_algalgrowth'])/2
                else:
                    p = (eia_row['S1_substrate'] + eia_row['S2_surfacewater'] + eia_row['S3_algalgrowth'])/3

                condition_score = v * 0.55 + h * 0.35 + p * 0.10

                ####### calculate EIA score
                eia_score = land_score * 0.3 + condition_score * 0.7

                ####### calculate EO score
                eo_score = eia_score
                # # Adjust EIA Rank based on Z1 Size
                if pd.notna(eia_row['size_rating']):
                    if eia_row['size_rating'] == 1:
                        eo_score = eia_score - 0.5
                    elif eia_row['size_rating'] == 3:
                        eo_score = eia_score - 0.25
                    elif eia_row['size_rating'] == 4:
                        eo_score = eia_score + 0.25
                        if eo_score > 5:
                            eo_score = 5
                    elif eia_row['size_rating'] == 5:
                        eo_score = eia_score + 0.5
                        if eo_score > 5:
                            eo_score = 5
                
                else:   # if no size ranking
                    eo_score=pd.NA
                return pd.Series([land_score, condition_score, eia_score, eo_score],
                                        index=["land_score", "condition_score", "eia_score", "eo_score"]) 

            self.eia_df[["land_score", "condition_score", "eia_score", "eo_score"]] = (
                self.eia_df.apply(compute_scores, axis=1)
            )


            def scores_to_grades(score):
                if pd.notna(score):
                    if score > 4.5:
                        return 'A'
                    elif score > 3.5:
                        return 'B'
                    elif score > 2.5:
                        return 'C'
                    else:
                        return 'D'
                return pd.NA

            for col in ["land_score", "condition_score", "eia_score", "eo_score"]:
                out_col = col[:-5] + "rank"
                self.eia_df[out_col] = self.eia_df[col].apply(scores_to_grades)


            ######################################
            # Modified from Bodie's 2024 script

            # Unpack mapping dictionaries
            def create_mapping_dict(df, code_col, desc_col):
                mapping_dict = pd.Series(df[desc_col].values, index=df[code_col]).to_dict()
                return mapping_dict

            # Create mapping dictionaries
            L1_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'L1'], 'EIA_Value', 'Description')
            L2_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'L2'], 'EIA_Value', 'Description')
            B1_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'B1'], 'EIA_Value', 'Description')
            B2_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'B2'], 'EIA_Value', 'Description')
            B3_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'B3'], 'EIA_Value', 'Description')
            B4_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'B4'], 'EIA_Value', 'Description')
            V1_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'V1'], 'EIA_Value', 'Description')
            V2_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'V2'], 'EIA_Value', 'Description')
            V3_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'V3'], 'EIA_Value', 'Description')
            V4_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'V4'], 'EIA_Value', 'Description')
            V5_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'V5'], 'EIA_Value', 'Description')
            V6_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'V6'], 'EIA_Value', 'Description')
            H1m_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'H1'], 'EIA_Value', 'Description')
            H2_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'H2'], 'EIA_Value', 'Description')
            H3_dictAll = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'H3'], 'EIA_Value', 'Description')
            S1_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'S1'], 'EIA_Value', 'Description')
            S2_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'S2'], 'EIA_Value', 'Description')
            S3_dict = create_mapping_dict(self.desc_df[self.desc_df['Field'] == 'S3'], 'EIA_Value', 'Description')

            landscape_comm_cols = ['BufferComments', 'Landscapecomments']

            # Create Landscape Context description
            self.eia_df['landscape_context_comm'] = (self.eia_df[landscape_comm_cols].apply(lambda r: ' '.join(r.dropna()), axis=1).str.strip())
            print(self.eia_df['landscape_context'])

            self.eia_df['landscape_context_auto']= (
                                                "EIA Landscape Rank = " + self.eia_df['land_rank'] + ';'
                                                + ' ' + "Metrics: " +
                                                "L1A: " + self.eia_df['L1'].map(L1_dict).fillna('')  + '; ' +
                                                "L2: " + self.eia_df['L2'].map(L2_dict).fillna('') + '; ' +
                                                "B1: " + self.eia_df['B1'].map(B1_dict).fillna('') + '; ' +
                                                "B2: " + self.eia_df['B2'].map(B2_dict).fillna('') + '; ' +
                                                "B3: " + self.eia_df['B3a'].map(B3_dict).fillna('') + '; ' +
                                                "B4: " + self.eia_df['B3b'].map(B4_dict).fillna('')
                                                )


            self.eia_df["H1_sources"] = (
                self.eia_df["H1_sources"]
                .str.replace("_", " ", regex=False)
                .str.replace(r",\s*", ", ", regex=True)
            )

            # Create Condition of EO description

            cond_comm_cols = [
                'VegetationCompositionComments',
                'VegetationStructureComments',
                'HydroperiodComments',
                'HydrologicConnectivityComments',
                'PhysiochemicalComments',
                'WaterSourceComments',
            ]

            base = (
                self.eia_df[cond_comm_cols]
                .astype("string")
                .apply(lambda r: ' '.join(r.dropna().str.strip()), axis=1)
            )

            h1 = self.eia_df["H1_sources"].astype("string").str.strip()
            h1 = h1.where(h1.notna() & (h1 != ""), "")

            self.eia_df["Water_source_desc"] = (
                np.where(h1 != "", " Water sources: " + h1, "")
            ).str.strip()





            self.eia_df['condition_auto'] = ("EIA Condition Rank = " + self.eia_df['condition_rank'] + ';' #this references the pre-calculated Condition Rank and will need to be replace with COE_Rank.
                                            + ' ' + "Metrics: " +
                                            "V1: " + self.eia_df['V1'].map(V1_dict).fillna('') + '; ' +
                                            "V2: " + self.eia_df['V2'].map(V2_dict).fillna('') + '; ' +
                                            "V3: " + self.eia_df['V3'].map(V3_dict).fillna('') + '; ' +
                                            "V4: " + self.eia_df['V4'].map(V4_dict).fillna('') + '; ' +
                                            "V5: " + self.eia_df['V5'].map(V5_dict).fillna('') + '; ' +
                                            "V6: " + self.eia_df['V6'].map(V6_dict).fillna('') + '; ' +
                                            "H1: " + self.eia_df['H1_metrics'].map(H1m_dict).fillna('') + '; ' +
                                            "H2: " + self.eia_df['H2_Hydroperiod'].map(H2_dict).fillna('') + '; ' +
                                            "H3: " + self.eia_df['H3'].map(H3_dictAll).fillna('') + '; ' +
                                            "S1: " + self.eia_df['S1_substrate'].map(S1_dict).fillna('') + '; ' +
                                            "S2: " + self.eia_df['S2_surfacewater'].map(S2_dict).fillna('') + '; ' +
                                            "S3: " + self.eia_df['S3_algalgrowth'].map(S3_dict).fillna('') +
                                            self.eia_df["Water_source_desc"]
            )


    def execute(self, parameters, messages):
        try:
            self.loadInputTable(parameters)
            self.loadEIADefinitions()
            self.summarizeEIA()

            # save output
            out_table = parameters[1].valueAsText
            # Save the updated DataFrame to a new CSV file
            out_excel = parameters[2].valueAsText

            if out_excel not in [None, ""]:
                self.eia_df.to_excel(out_excel, index=False)
            self.eia_df.to_csv(out_excel, index=False)
            print(f"EIA descriptions have been successfully written to {out_excel}")

            arcpy.AddMessage("Writing output table...")

            ## Replace pandas NA with None so ArcGIS accepts it
            # Replace missing values
            self.eia_df = self.eia_df.where(self.eia_df.notna(), None)

            # Cast numerics properly
            for col in self.eia_df.select_dtypes(include=["Int64", "float"]):
                self.eia_df[col] = self.eia_df[col].astype(float)

            # Cast object/string columns
            for col in self.eia_df.select_dtypes(include=["object", "string"]):
                self.eia_df[col] = self.eia_df[col].astype(str)

            # Convert to NumPy record array
            out_array = np.rec.fromrecords(self.eia_df.to_numpy(), names=list(self.eia_df.columns))

            # Delete output table if exists
            if arcpy.Exists(out_table):
                arcpy.management.Delete(out_table)

            # Write to GDB
            arcpy.da.NumPyArrayToTable(out_array, out_table)

            arcpy.AddMessage("Done!")

        except Exception as e:
            tb = traceback.format_exc()
            arcpy.AddError("Tool failed!")
            arcpy.AddError(tb)
            raise
