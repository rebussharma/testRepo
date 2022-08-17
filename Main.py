import psycopg2
from psycopg2.extras import RealDictCursor
from pprint import pprint

def handler(event, context):

    # RealDictCursor turns the result set into a dictionary so you can reference columns by their names.
    connection = psycopg2.connect(
        host="pcatestdb-post.cxbtzodtg0ei.us-gov-west-1.rds.amazonaws.com",
        database="apcas_hs_config",
        user="hs_login",
        port="5432",
        password="Wdfgrbic53lap!2S",
        cursor_factory=RealDictCursor)

    # Create a cursor to perform database operations.
    cursor = connection.cursor()

    selectQuery = """SELECT 
        submission.id as "Submission ID", 
        submission.uuid as "Submission UUID",
        submission.external_id as "Submission Externam ID" ,
        submission.dt_created as "Submission Date Created", 
        submission.dt_completed as "Submission Date Completed", 
        min(submissionLog.activity_start) as "Submission Date Started", 
        job.uuid as "Job UUID", 
        job.name as "Job Name", 
        job.dt_started as "Job Date Started", 
        job.dt_completed as "Job Date Completed", 
        job.parent_id AS "Parent ID",
        pageNumberTable.numpages AS "Number of Pages"
    
    FROM 
        form_extraction_submission submission
    
    
    --JOIN to get the Date Started of a submission (Earliest date that a job started) 
    JOIN 
        reports_submissionlog submissionLog on submission.id = submissionLog.submission_id
    
    --JOIN to get job details
    JOIN 
        hyperflow_wfeworkflowinstance job on (job.correlation_id = submission."uuid"::text)
    
    -- JOIN to get the number of pages per submission
    JOIN 
        (SELECT 
            submission.id, 
            count(submissionPages.id) AS "numpages"
            FROM form_extraction_submission submission 
            JOIN form_extraction_submissionpage submissionPages on submissionPages.submission_id = submission.id
            WHERE submission.id  in (1141625, 1141627, 1141626)
            GROUP BY submission.id) AS pageNumberTable ON pageNumberTable.id = submission.id
   
    WHERE 
        submission.id  in (1141625, 1141627, 1141626)
    GROUP BY 
        submission.id, pageNumberTable.numpages, JOB."uuid"
    ORDER BY 
        submission.id, job.dt_started"""

    # Execute the query and get the result set.
    cursor.execute(selectQuery)
    records = cursor.fetchall()

    # Parent object
    submissionData = {"submissions": []}

    # Keep track of if we're still working on the same submission object or a new submission object.
    submissionID = None

    # External submission object, so we always have a reference to the current submission who's jobs are being worked
    # through
    submission = None

    # List of unique submission objects that have been worked on. Will be put into the parent object at the end.
    submissionAccumulator = []

    for record in records:

        # If it's the very first submission OR we have iterated into a new submission that we haven't processed yet
        # create a new submission object.
        if submissionID is None or submissionID != record["Submission ID"]:
            submissionID = record["Submission ID"]

            submission = {"id": record["Submission ID"], "uuid": record["Submission UUID"],
                          "dateCreated": str(record["Submission Date Created"]),
                          "dateStarted": str(record["Submission Date Started"]),
                          "dateCompleted": str(record["Submission Date Completed"]),
                          "numPages": record["Number of Pages"], "jobs": []}

            # Append this new submission object to a list that will keep track of all the submissions so far
            # This same submission object will continue to expand its "jobs" list as we continue iterating.
            submissionAccumulator.append(submission)

        job = {"uuid": record["Job UUID"], "name": record["Job Name"], "dateStarted": str(record["Job Date Started"]),
               "dateCompleted": str(record["Job Date Completed"]), "parentId": record["Parent ID"],
               "jobExecutionNodeId": None}

        submission["jobs"].append(job)
    submissionData["submissions"] = submissionAccumulator
    pprint(submissionData)
