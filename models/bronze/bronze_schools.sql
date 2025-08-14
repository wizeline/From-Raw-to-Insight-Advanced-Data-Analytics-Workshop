{{
  config(
    materialized='view',
    schema='bronze'
  )
}}

-- Bronze layer: Raw Chicago Public Schools data
-- This model ingests raw data with minimal transformation
select 
    School_ID,
    NAME_OF_SCHOOL,
    "Elementary, Middle, or High School" as school_type,
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
    AVERAGE_STUDENT_ATTENDANCE,
    Rate_of_Misconducts__per_100_students_,
    Average_Teacher_Attendance,
    Individualized_Education_Program_Compliance_Rate,
    COMMUNITY_AREA_NUMBER,
    COMMUNITY_AREA_NAME,
    Ward,
    Police_District,
    X_COORDINATE,
    Y_COORDINATE,
    Latitude,
    Longitude,
    Location,
    current_timestamp() as ingested_at
from {{ source('raw_data', 'chicago_public_schools') }}
