{{
  config(
    materialized='view',
    schema='silver'
  )
}}

-- Silver layer: Cleaned and standardized Chicago Public Schools data
-- This model applies data quality checks and standardization
with bronze_schools as (
    select * from {{ ref('bronze_schools') }}
),

-- Step 1: Clean NDA values
nda_cleaned_schools as (
    select 
        School_ID,
        NAME_OF_SCHOOL,
        school_type,
        Street_Address,
        City,
        State,
        ZIP_Code,
        Phone_Number,
        Link,
        Network_Manager,
        Collaborative_Name,
        Adequate_Yearly_Progress_Made_,
        Track_Schedule,
        CPS_Performance_Policy_Status,
        CPS_Performance_Policy_Level,
        HEALTHY_SCHOOL_CERTIFIED,
        Safety_Icon,
        {{ clean_nda_values('SAFETY_SCORE') }} as SAFETY_SCORE,
        Family_Involvement_Icon,
        {{ clean_nda_values('Family_Involvement_Score') }} as Family_Involvement_Score,
        Environment_Icon,
        {{ clean_nda_values('Environment_Score') }} as Environment_Score,
        Instruction_Icon,
        {{ clean_nda_values('Instruction_Score') }} as Instruction_Score,
        Leaders_Icon,
        {{ clean_nda_values('Leaders_Score') }} as Leaders_Score,
        Teachers_Icon,
        {{ clean_nda_values('Teachers_Score') }} as Teachers_Score,
        Parent_Engagement_Icon,
        {{ clean_nda_values('Parent_Engagement_Score') }} as Parent_Engagement_Score,
        Parent_Environment_Icon,
        {{ clean_nda_values('Parent_Environment_Score') }} as Parent_Environment_Score,
        {{ clean_nda_values('AVERAGE_STUDENT_ATTENDANCE') }} as AVERAGE_STUDENT_ATTENDANCE,
        {{ clean_nda_values('Rate_of_Misconducts__per_100_students_') }} as Rate_of_Misconducts__per_100_students_,
        {{ clean_nda_values('Average_Teacher_Attendance') }} as Average_Teacher_Attendance,
        {{ clean_nda_values('Individualized_Education_Program_Compliance_Rate') }} as Individualized_Education_Program_Compliance_Rate,
        COMMUNITY_AREA_NUMBER,
        COMMUNITY_AREA_NAME,
        Ward,
        Police_District,
        X_COORDINATE,
        Y_COORDINATE,
        Latitude,
        Longitude,
        Location,
        ingested_at
    from bronze_schools
),

-- Step 2: Clean percentage columns by removing % signs
percentage_cleaned_schools as (
    select 
        School_ID,
        NAME_OF_SCHOOL,
        school_type,
        Street_Address,
        City,
        State,
        ZIP_Code,
        Phone_Number,
        Link,
        Network_Manager,
        Collaborative_Name,
        Adequate_Yearly_Progress_Made_,
        Track_Schedule,
        CPS_Performance_Policy_Status,
        CPS_Performance_Policy_Level,
        HEALTHY_SCHOOL_CERTIFIED,
        Safety_Icon,
        SAFETY_SCORE,
        Family_Involvement_Icon,
        Family_Involvement_Score,
        Environment_Icon,
        Environment_Score,
        Instruction_Icon,
        Instruction_Score,
        Leaders_Icon,
        Leaders_Score,
        Teachers_Icon,
        Teachers_Score,
        Parent_Engagement_Icon,
        Parent_Engagement_Score,
        Parent_Environment_Icon,
        Parent_Environment_Score,
        -- Remove % signs and convert to numeric
        try_cast(replace(AVERAGE_STUDENT_ATTENDANCE, '%', '') as float) as AVERAGE_STUDENT_ATTENDANCE,
        Rate_of_Misconducts__per_100_students_,
        -- Remove % signs and convert to numeric
        try_cast(replace(Average_Teacher_Attendance, '%', '') as float) as Average_Teacher_Attendance,
        -- Remove % signs and convert to numeric
        try_cast(replace(Individualized_Education_Program_Compliance_Rate, '%', '') as float) as Individualized_Education_Program_Compliance_Rate,
        COMMUNITY_AREA_NUMBER,
        COMMUNITY_AREA_NAME,
        Ward,
        Police_District,
        X_COORDINATE,
        Y_COORDINATE,
        Latitude,
        Longitude,
        Location,
        ingested_at
    from nda_cleaned_schools
),

-- Step 3: Apply data validation and standardization
standardized_schools as (
    select 
        School_ID,
        NAME_OF_SCHOOL,
        school_type,
        Street_Address,
        City,
        State,
        ZIP_Code,
        Phone_Number,
        Link,
        Network_Manager,
        Collaborative_Name,
        Adequate_Yearly_Progress_Made_,
        Track_Schedule,
        CPS_Performance_Policy_Status,
        CPS_Performance_Policy_Level,
        HEALTHY_SCHOOL_CERTIFIED,
        Safety_Icon,
        case 
            when SAFETY_SCORE < 0 or SAFETY_SCORE > 100 then null
            else SAFETY_SCORE 
        end as safety_score,
        Family_Involvement_Icon,
        case 
            when Family_Involvement_Score < 0 or Family_Involvement_Score > 100 then null
            else Family_Involvement_Score 
        end as family_involvement_score,
        Environment_Icon,
        case 
            when Environment_Score < 0 or Environment_Score > 100 then null
            else Environment_Score 
        end as environment_score,
        Instruction_Icon,
        case 
            when Instruction_Score < 0 or Instruction_Score > 100 then null
            else Instruction_Score 
        end as instruction_score,
        Leaders_Icon,
        case 
            when Leaders_Score < 0 or Leaders_Score > 100 then null
            else Leaders_Score 
        end as leaders_score,
        Teachers_Icon,
        case 
            when Teachers_Score < 0 or Teachers_Score > 100 then null
            else Teachers_Score 
        end as teachers_score,
        Parent_Engagement_Icon,
        case 
            when Parent_Engagement_Score < 0 or Parent_Engagement_Score > 100 then null
            else Parent_Engagement_Score 
        end as parent_engagement_score,
        Parent_Environment_Icon,
        case 
            when Parent_Environment_Score < 0 or Parent_Environment_Score > 100 then null
            else Parent_Environment_Score 
        end as parent_environment_score,
        case 
            when AVERAGE_STUDENT_ATTENDANCE < 0 or AVERAGE_STUDENT_ATTENDANCE > 100 then null
            else AVERAGE_STUDENT_ATTENDANCE 
        end as average_student_attendance,
        case 
            when Rate_of_Misconducts__per_100_students_ < 0 then null
            else Rate_of_Misconducts__per_100_students_ 
        end as misconduct_rate_per_100_students,
        case 
            when Average_Teacher_Attendance < 0 or Average_Teacher_Attendance > 100 then null
            else Average_Teacher_Attendance 
        end as average_teacher_attendance,
        case 
            when Individualized_Education_Program_Compliance_Rate < 0 or Individualized_Education_Program_Compliance_Rate > 100 then null
            else Individualized_Education_Program_Compliance_Rate 
        end as iep_compliance_rate,
        case 
            when COMMUNITY_AREA_NUMBER = 0 or COMMUNITY_AREA_NUMBER is null then null
            else COMMUNITY_AREA_NUMBER 
        end as community_area_number,
        COMMUNITY_AREA_NAME,
        case 
            when Ward = 0 or Ward is null then null
            else Ward 
        end as ward,
        case 
            when Police_District = 0 or Police_District is null then null
            else Police_District 
        end as police_district,
        case 
            when X_COORDINATE = 0 or X_COORDINATE is null then null
            else X_COORDINATE 
        end as x_coordinate,
        case 
            when Y_COORDINATE = 0 or Y_COORDINATE is null then null
            else Y_COORDINATE 
        end as y_coordinate,
        case 
            when Latitude = 0 or Latitude is null then null
            else Latitude 
        end as latitude,
        case 
            when Longitude = 0 or Longitude is null then null
            else Longitude 
        end as longitude,
        Location,
        ingested_at
    from percentage_cleaned_schools
    where School_ID is not null  -- Remove records without school ID
)

select * from standardized_schools
