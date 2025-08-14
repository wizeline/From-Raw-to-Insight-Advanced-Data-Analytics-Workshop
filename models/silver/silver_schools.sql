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

cleaned_schools as (
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
    from bronze_schools
    where School_ID is not null  -- Remove records without school ID
)

select * from cleaned_schools
