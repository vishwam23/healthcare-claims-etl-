# Databricks notebook source
# DBTITLE 1,🚀 Git Workflow Guide - How to Push Changes to GitHub
# MAGIC %md
# MAGIC # 🚀 Git Workflow Guide
# MAGIC
# MAGIC ## How to Make Changes & Push to GitHub
# MAGIC
# MAGIC This notebook shows you **exactly** how to work with your Git repository in Databricks.
# MAGIC
# MAGIC ### Your Repository
# MAGIC - **GitHub URL**: https://github.com/vishwam23/healthcare-claims-etl-
# MAGIC - **Local Path**: `/Users/vishwam23042002@gmail.com/healthcare-claims-etl`

# COMMAND ----------

# DBTITLE 1,Step 1: Check Git Status (What Changed?)
# Import necessary library
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

# Your Git repo path
repo_path = "/Users/vishwam23042002@gmail.com/healthcare-claims-etl"

print("📂 Checking Git status...\n")
print(f"Repository: {repo_path}")
print("="*60)

# Check status using filesystem
try:
    files = dbutils.fs.ls(f"/Workspace{repo_path}")
    print(f"\n✓ Found {len(files)} items in repository")
    for f in files:
        print(f"  - {f.name}")
except Exception as e:
    print(f"⚠ Error: {e}")

# COMMAND ----------

# DBTITLE 1,Step 2: Make Your Changes (Example)
# MAGIC %md
# MAGIC ## Step 2: Make Your Changes
# MAGIC
# MAGIC You can:
# MAGIC 1. **Edit existing notebooks** in `/notebooks/` folder
# MAGIC 2. **Add new notebooks** or files
# MAGIC 3. **Update README.md**
# MAGIC 4. **Modify data folder structure**
# MAGIC
# MAGIC ### Example Changes:
# MAGIC - Add a new transformation in `silver.py`
# MAGIC - Update documentation in `README.md`
# MAGIC - Create a new analysis notebook
# MAGIC - Add sample data files
# MAGIC
# MAGIC **💡 Tip**: After making changes, come back here to commit!

# COMMAND ----------

# DBTITLE 1,Step 3: Review Your Changes
# This would normally show git status
# Since we're using Projects API, we'll show the concept

print("📋 Review Changes Before Committing")
print("="*60)
print("\nCommands to check status:")
print("\n1. Using Projects API (what we use):")
print("   Status shows: branch, changed files, clean state")
print("\n2. Manual check:")
print("   - Look at your edited notebooks")
print("   - Check new files you created")
print("   - Review any deletions")
print("\n3. Git Panel in UI:")
print("   - Click Git icon (left sidebar)")
print("   - See visual diff of changes")
print("="*60)

# COMMAND ----------

# DBTITLE 1,Step 4: Commit & Push to GitHub (THE MAIN COMMAND)
# ⚠️ IMPORTANT: Edit the commit message below before running!

# Your commit message (describe what you changed)
commit_message = "Add new feature to gold layer"  # ← CHANGE THIS!

repo_path = "/Users/vishwam23042002@gmail.com/healthcare-claims-etl"

print("🔄 Preparing to commit and push...\n")
print(f"Repository: {repo_path}")
print(f"Message: {commit_message}")
print("="*60)

# Uncomment the lines below when ready to push:

# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()
# 
# result = w.repos.update(
#     repo_id=<YOUR_REPO_ID>,  # You'll get this from Repos
#     branch="main"
# )
# 
# print("✓ Changes pushed to GitHub!")

print("\n⚠️ This is a template. For actual push, see Step 5!")

# COMMAND ----------

# DBTITLE 1,Step 5: EASY METHOD - Use Git Panel in UI
# MAGIC %md
# MAGIC ## ✅ RECOMMENDED: Use Git Panel (Easiest Way)
# MAGIC
# MAGIC ### Visual Steps:
# MAGIC
# MAGIC 1. **Open your Git folder**:
# MAGIC    - Navigate to: `/Users/vishwam23042002@gmail.com/healthcare-claims-etl`
# MAGIC
# MAGIC 2. **Click Git icon** (left sidebar):
# MAGIC    - Shows changed files in orange
# MAGIC    - Shows new files with `+` symbol
# MAGIC
# MAGIC 3. **Review changes**:
# MAGIC    - Click on any file to see diff
# MAGIC    - Make sure changes look correct
# MAGIC
# MAGIC 4. **Commit**:
# MAGIC    - Click "Commit" button
# MAGIC    - Enter descriptive message
# MAGIC    - Example: "Added member analysis"
# MAGIC
# MAGIC 5. **Push to GitHub**:
# MAGIC    - Click "Push" button
# MAGIC    - Your changes go to GitHub!
# MAGIC
# MAGIC 6. **Verify**:
# MAGIC    - Go to https://github.com/vishwam23/healthcare-claims-etl-
# MAGIC    - See your new commit!

# COMMAND ----------

# DBTITLE 1,Working with Branches (Advanced)
# MAGIC %md
# MAGIC ## 🌿 Working with Branches
# MAGIC
# MAGIC ### Why Use Branches?
# MAGIC - **main**: Production code (always working)
# MAGIC - **dev**: Development/testing
# MAGIC - **feature/xyz**: New features
# MAGIC
# MAGIC ### Create New Branch:
# MAGIC
# MAGIC **In UI:**
# MAGIC 1. Open Git panel
# MAGIC 2. Click branch dropdown
# MAGIC 3. Type new branch name
# MAGIC 4. Click "Create branch"
# MAGIC
# MAGIC **In Code:**
# MAGIC ```python
# MAGIC # Create and switch to new branch
# MAGIC # runGit(operation="checkout", 
# MAGIC #        repoPath="/Users/vishwam23042002@gmail.com/healthcare-claims-etl",
# MAGIC #        branchName="dev")
# MAGIC ```
# MAGIC
# MAGIC ### Branch Workflow:
# MAGIC 1. Create branch: `git checkout -b feature/new-analysis`
# MAGIC 2. Make changes
# MAGIC 3. Commit to branch
# MAGIC 4. Push branch
# MAGIC 5. Create Pull Request on GitHub
# MAGIC 6. Merge to main after review

# COMMAND ----------

# DBTITLE 1,Common Git Commands Reference
print("="*60)
print("📚 COMMON GIT COMMANDS REFERENCE")
print("="*60)

commands = {
    "Check Status": "See what files changed",
    "Create Branch": "Work on new feature separately",
    "Commit": "Save your changes locally",
    "Push": "Send changes to GitHub",
    "Pull": "Get latest changes from GitHub",
    "Merge": "Combine branches"
}

for cmd, desc in commands.items():
    print(f"\n{cmd:15s} → {desc}")

print("\n" + "="*60)
print("\n💡 QUICK REFERENCE:")
print("""
1. Daily workflow:
   - Make changes → Check status → Commit → Push

2. Team workflow:
   - Pull latest → Create branch → Make changes → 
     Commit → Push → Create PR → Merge

3. Emergency fix:
   - Create hotfix branch → Fix → Test → 
     Commit → Push → Merge to main
""")
print("="*60)

# COMMAND ----------

# DBTITLE 1,🎯 Quick Start Template (Copy & Modify This!)
# ==========================================
# YOUR QUICK COMMIT & PUSH TEMPLATE
# ==========================================
# Copy this cell and modify for each commit!

repo_path = "/Users/vishwam23042002@gmail.com/healthcare-claims-etl"

# 📝 STEP 1: Describe what you changed
commit_message = """Add new data quality checks

- Added validation for claim amounts
- Updated silver layer logic
- Fixed date formatting issue
"""

# ✅ STEP 2: Run this cell to commit and push
print("🔄 Ready to commit with message:")
print("="*60)
print(commit_message)
print("="*60)
print("\n⚠️ To push: Use Git panel or uncomment code below")

# Uncomment when ready:
# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()
# # Use Git panel instead for easier workflow!

print("\n💡 TIP: Use Git Panel in UI - it's easier!")

# COMMAND ----------

# DBTITLE 1,✅ Checklist Before Pushing
# MAGIC %md
# MAGIC ## ✅ Pre-Push Checklist
# MAGIC
# MAGIC Before pushing to GitHub, verify:
# MAGIC
# MAGIC ### Code Quality:
# MAGIC - [ ] Code runs without errors
# MAGIC - [ ] Tested on sample data
# MAGIC - [ ] Comments added for complex logic
# MAGIC - [ ] No hardcoded credentials
# MAGIC
# MAGIC ### Documentation:
# MAGIC - [ ] README updated if needed
# MAGIC - [ ] New features documented
# MAGIC - [ ] Function docstrings added
# MAGIC
# MAGIC ### Git:
# MAGIC - [ ] Meaningful commit message
# MAGIC - [ ] Right branch selected
# MAGIC - [ ] No unnecessary files (temp, logs)
# MAGIC - [ ] Reviewed changes in Git panel
# MAGIC
# MAGIC ### Team:
# MAGIC - [ ] Communicated major changes
# MAGIC - [ ] PR created if required
# MAGIC - [ ] Tests pass
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎉 You're Ready!
# MAGIC
# MAGIC Now you know how to:
# MAGIC 1. ✅ Check what changed
# MAGIC 2. ✅ Commit your work
# MAGIC 3. ✅ Push to GitHub
# MAGIC 4. ✅ Work with branches
# MAGIC 5. ✅ Explain it to others!

# COMMAND ----------

