# Project 2: Analysis of Twitter Data
Ronald Hernandez, Romell Pineda, Michael Stanco

## Main/Broad Questions to Answer
Are tweets generally positive or negative?<br/>
What makes a tweet positive or negative in nature?<br/>

## More Specific Questions to Answer
We want to find factors (such as hashtags, specific keywords, emojis, etc.) that are indicative of a tweet being deemed ‘positive’ or ‘negative’. We plan on setting up a table where we test/list factors that are accurate in labeling a tweet positive or negative. Using these factors, we will try to answer the following questions:
- Within our criteria, are we able to accurately quantify a positive or negative tweet?
- Does time of day play a role in tweet positivity/negativity?
- Do active users tend to become more positive/negative?
- Does age of a user account play a role in tweet positivity/negativity?
- Are tweets from celebrities generally more negative than positive?
- Can an observation be made about the correlation between an account’s followers and tweet positivity/negativity?

## Procedure
1) Understand twitter API data
2) Find twitter data repository
3) Establish criteria for positive/negative tweets
4) Read in and process data from online twitter API
5) Set-up code/queries to perform analysis
6) Set-up cloud computing, such as AWS/EC2/SS3
7) Submit to cloud computing platform(s) and run analysis
8) Extract analysis/findings
9) Compile all findings and conclusions into a final presentation

## Technologies Used
Git/Github<br/>
Trello<br/>
Scala/Spark<br/>
Hive <br/>
YARN<br/>
AWS/SS3<br/>
Twitter API<br/>
Zoom/Discord<br/>

## Work Flow
Recommended work flow for code changes is as follows:

From **main** branch run

`git pull`

Create new feature branch with

`git checkout -b my_branchname-developer_alias`

Make code changes and then...

```
git add my_changes
git commit -m 'added my changes'
git checkout main
git pull
git checkout my_branchname
git rebase main -i
```
> Squash commits and resolve merge conflicts as needed. Refer to the respective sections below for additional information

```
git checkout main
git push origin my_branchname
```
Create a pull request on GitHub and notify team member of pending review


## Squashing Commits
We will want to squash commits in order to keep the project history streamlined.  When you run `git rebase main -i` a panel in your default text editor will open. Refer to the image below.
> ![text editor panel example](assets/picks.png)
If the text within the panel appears similar to the image above with multiple "pick" lines, change all but one line to "squash"

It should now resemble the image below. Save and close the panel in order to continue the rebase process. 
> ![squashed commits](assets/squash.png)

## Resolving Merge Conflicts
In general, if resolving merge conflicts is not going well, you can run `git rebase --abort` any time during the process to undo your recent changes and start over. The following steps are a high-level outline and may vary depending on your unique situation.

- Open the project in your preferred text editor
- Lines or blocks of code that contain conflicts should be highlighted
- Options to accept current change, accept incoming change, accept both changes, and compare changes should be available
- Select the appropriate change. If uncertain, consult with a team member
- Save and run `git add <my_changes>` (committing is not necessary)
- It is good practice to check other files you have altered to see if any other conflicts exist
- run `git rebase --continue` to complete the rebase process

## Getting Started
1. Install all required dependencies with correct versions.
2. Clone the repository to your local environment.
3. Acquire a Twitter developer account and a bearer token for authentication.
4. Run the command provided in the preferences.txt file on your Mac terminal or Windows command prompt.
5. From the either the terminal or command prompt navigate to the project directory where the build.sbt file resides.
6. Run
```
sbt clean
sbt reload
sbt assembly
park-submit target/scala-2.11/twitter_data-assembly-0.1.0-SNAPSHOT.jar
```

## Conflict Resolution Plan
In the unlikely event that team members encounter an unresolvable issue between themselves, a knowledgeable, neutral third-party will be consulted to aid in resolution.  However, team members are encouraged to resolve all issues internally.   

## License

This project uses the following license: [MIT License]("https://opensource.org/licenses/MIT").
