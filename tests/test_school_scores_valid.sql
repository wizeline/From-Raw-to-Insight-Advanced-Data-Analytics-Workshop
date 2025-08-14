-- Custom test to ensure school scores are within valid ranges (0-100)
select *
from {{ ref('silver_schools') }}
where (safety_score < 0 or safety_score > 100)
   or (family_involvement_score < 0 or family_involvement_score > 100)
   or (environment_score < 0 or environment_score > 100)
   or (instruction_score < 0 or instruction_score > 100)
   or (leaders_score < 0 or leaders_score > 100)
   or (teachers_score < 0 or teachers_score > 100)
   or (parent_engagement_score < 0 or parent_engagement_score > 100)
   or (parent_environment_score < 0 or parent_environment_score > 100)
   or (average_student_attendance < 0 or average_student_attendance > 100)
   or (average_teacher_attendance < 0 or average_teacher_attendance > 100)
   or (iep_compliance_rate < 0 or iep_compliance_rate > 100)
