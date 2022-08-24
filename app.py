import psycopg2
from psycopg2.extras import RealDictCursor
from pprint import pprint


def handler(event, context):
    # RealDictCursor turns the result set into a dictionary so you can reference columns by their names.
    print('Establishing connection')
    connection = ""
    connection_success = False
    try:
        connection = psycopg2.connect(
            host="pcatestdb-post.cxbtzodtg0ei.us-gov-west-1.rds.amazonaws.com",
            database="apcas_hs_config",
            user="hs_login",
            port="5432",
            password="Wdfgrbic53lap!2S",
            cursor_factory=RealDictCursor)
        connection_success = True
    except (Exception, psycopg2.OperationalError) as error:
        print('Connection failed with error: ', error)
        connection_success = False

    if connection_success:
        cursor = connection.cursor()

        selectQuery = """SELECT 
            submission.id as "Submission ID", 
            submission.uuid as "Submission UUID", 
            submission.dt_created as "Submission Date Created", 
            submission.dt_completed as "Submission Date Completed", 
            min(submissionLog.activity_start) as "Submission Date Started", 
            workflow.uuid as "Workflow UUID", 
            workflow.name as "Workflow Name", 
            workflow.dt_started as "Workflow Date Started", 
            workflow.dt_completed as "Workflow Date Completed", 
            workflow.parent_id AS "Parent ID",
            pageNumberTable.numpages AS "Number of Pages"
    
        FROM 
            form_extraction_submission submission
    
        --JOIN to get the Date Started of a submission (Earliest date that a workflow started) 
        JOIN 
            reports_submissionlog submissionLog on submission.id = submissionLog.submission_id
    
        --JOIN to get workflow details
        JOIN 
            hyperflow_wfeworkflowinstance workflow on (workflow.correlation_id = submission."uuid"::text)
    
        -- JOIN to get the number of pages per submission
        JOIN 
            (SELECT 
                submission.id, 
                count(submissionPages.id) AS "numpages"
                FROM form_extraction_submission submission 
                JOIN form_extraction_submissionpage submissionPages on submissionPages.submission_id = submission.id
                WHERE submission.id in %s
                GROUP BY submission.id) AS pageNumberTable ON pageNumberTable.id = submission.id
        WHERE 
            submission.id in %s
        GROUP BY 
            submission.id, pageNumberTable.numpages, WORKFLOW."uuid"
        ORDER BY 
            submission.id, workflow.dt_started"""

        # Execute the query and get the result set.
        data = (('1141625', '1141627', '1141626'), ('1141625', '1141627', '1141626'))
        cursor.execute(selectQuery, data)
        # Create a cursor to perform database operations.
        records = cursor.fetchall()

        # Parent object
        submissionData = {"submissions": []}

        # Keep track of if we're still working on the same submission object or a new submission object.
        submissionID = None

        # External submission object so we always have a reference to the current submission who's workflows are being worked through.
        submission = None

        # List of unique submission objects that have been worked on. Will be put into the parent object at the end.
        submissionAccumulator = []

        for record in records:

            # If it's the very first submission (ID=None) OR we have iterated into a new submission
            # create a new submission object.
            if submissionID is None or submissionID != record["Submission ID"]:
                submissionID = record["Submission ID"]

                submission = {}
                submission["id"] = record["Submission ID"]
                submission["uuid"] = record["Submission UUID"]
                submission["dateCreated"] = str(record["Submission Date Created"])
                submission["dateStarted"] = str(record["Submission Date Started"])
                submission["dateCompleted"] = str(record["Submission Date Completed"])
                submission["numPages"] = record["Number of Pages"]
                submission["workflows"] = []

                # Append this new submission object to a list that will keep track of all of the submissions so far
                # This same submission object will continue to expand its "workflows" list as we continue iterating.
                submissionAccumulator.append(submission)

            workflow = {}
            workflow["uuid"] = record["Workflow UUID"]
            workflow["name"] = record["Workflow Name"]
            workflow["dateStarted"] = str(record["Workflow Date Started"])
            workflow["dateCompleted"] = str(record["Workflow Date Completed"])
            workflow["parentId"] = record["Parent ID"]
            workflow["workflowExecutionNodeId"] = None

            submission["workflows"].append(workflow)

        submissionData["submissions"] = submissionAccumulator
        pprint(submissionData)
