#!/bin/bash

# =============================================================================
# GITHUB DEPLOYMENT SCRIPT
# =============================================================================
# Project: Big Data Pipeline for Diabetes Prediction
# Team: Kelompok 8 RA
# =============================================================================

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
print_header() {
    echo -e "\n${BLUE}=== $1 ===${NC}\n"
}

print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Configuration
REPO_NAME="diabetes-prediction-pipeline"
REPO_DESCRIPTION="Big Data Pipeline for Diabetes Prediction using Apache Spark, Airflow, and ML"
REPO_TOPICS="data-engineering,apache-spark,airflow,machine-learning,diabetes-prediction,docker,big-data"
DEFAULT_BRANCH="main"

# Function to check prerequisites
check_prerequisites() {
    print_header "🔍 Checking Prerequisites"
    
    # Check if git is installed
    if ! command -v git &> /dev/null; then
        print_error "Git is not installed. Please install Git first."
        exit 1
    fi
    print_status "Git is installed: $(git --version)"
    
    # Check if GitHub CLI is installed
    if ! command -v gh &> /dev/null; then
        print_warning "GitHub CLI (gh) is not installed."
        print_info "You can install it from: https://cli.github.com/"
        print_info "Continuing with manual setup instructions..."
        GITHUB_CLI_AVAILABLE=false
    else
        print_status "GitHub CLI is installed: $(gh --version | head -n1)"
        GITHUB_CLI_AVAILABLE=true
        
        # Check if user is authenticated
        if ! gh auth status &> /dev/null; then
            print_warning "Not authenticated with GitHub CLI"
            print_info "Run: gh auth login"
            GITHUB_CLI_AUTHENTICATED=false
        else
            print_status "GitHub CLI is authenticated"
            GITHUB_CLI_AUTHENTICATED=true
        fi
    fi
    
    # Check if in git repository
    if ! git rev-parse --git-dir &> /dev/null; then
        print_warning "Not in a git repository. Initializing..."
        git init
        print_status "Git repository initialized"
    else
        print_status "Already in a git repository"
    fi
}

# Function to validate project structure
validate_project_structure() {
    print_header "📁 Validating Project Structure"
    
    required_files=(
        "README.md"
        "docker-compose.yml"
        ".env.example"
        ".gitignore"
        "Makefile"
        "CONTRIBUTING.md"
        "SECURITY.md"
        "CODEOWNERS"
    )
    
    required_dirs=(
        "scripts"
        "airflow/dags"
        "docker"
        "tests"
        "data/bronze"
        "data/silver" 
        "data/gold"
        ".github/workflows"
        ".github/ISSUE_TEMPLATE"
    )
    
    print_info "Checking required files..."
    for file in "${required_files[@]}"; do
        if [[ -f "$file" ]]; then
            print_status "$file exists"
        else
            print_error "$file is missing"
            exit 1
        fi
    done
    
    print_info "Checking required directories..."
    for dir in "${required_dirs[@]}"; do
        if [[ -d "$dir" ]]; then
            print_status "$dir/ exists"
        else
            print_warning "$dir/ is missing - creating..."
            mkdir -p "$dir"
        fi
    done
}

# Function to clean up repository
cleanup_repository() {
    print_header "🧹 Cleaning Up Repository"
    
    # Remove development artifacts
    print_info "Removing Python cache files..."
    find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    find . -name "*.pyc" -delete 2>/dev/null || true
    find . -name ".DS_Store" -delete 2>/dev/null || true
    
    # Clean up any local environment files
    if [[ -f ".env" ]]; then
        print_warning "Found .env file - this should not be committed"
        print_info "Ensure .env is in .gitignore"
    fi
    
    # Check for large files
    print_info "Checking for large files..."
    large_files=$(find . -type f -size +100M 2>/dev/null | grep -v ".git" | head -5)
    if [[ -n "$large_files" ]]; then
        print_warning "Large files found (>100MB):"
        echo "$large_files"
        print_info "Consider using Git LFS for large files"
    fi
    
    print_status "Repository cleanup completed"
}

# Function to run security checks
run_security_checks() {
    print_header "🔒 Running Security Checks"
    
    # Check for potential secrets
    print_info "Scanning for potential secrets..."
    secret_patterns=(
        "password"
        "secret"
        "key"
        "token"
        "api_key"
        "private_key"
    )
    
    for pattern in "${secret_patterns[@]}"; do
        matches=$(grep -r -i "$pattern" . --exclude-dir=.git --exclude-dir=test_env --exclude="*.md" --exclude="deploy_to_github.sh" | grep -v ".example" | head -3)
        if [[ -n "$matches" ]]; then
            print_warning "Found potential secrets with pattern '$pattern':"
            echo "$matches"
        fi
    done
    
    # Check .gitignore effectiveness
    print_info "Validating .gitignore..."
    if git status --porcelain --ignored | grep "^!!" | head -5; then
        print_status ".gitignore is working - ignored files found"
    fi
    
    print_status "Security checks completed"
}

# Function to prepare git repository
prepare_git_repository() {
    print_header "📝 Preparing Git Repository"
    
    # Configure git settings if not set
    if ! git config user.name &> /dev/null; then
        print_warning "Git user.name not set"
        read -p "Enter your Git username: " git_username
        git config user.name "$git_username"
    fi
    
    if ! git config user.email &> /dev/null; then
        print_warning "Git user.email not set"
        read -p "Enter your Git email: " git_email
        git config user.email "$git_email"
    fi
    
    print_status "Git user: $(git config user.name) <$(git config user.email)>"
    
    # Check if there are any commits
    if ! git rev-parse --verify HEAD &> /dev/null; then
        print_info "No commits found - creating initial commit..."
        
        # Add all files
        git add .
        
        # Create initial commit
        git commit -m "feat: initial commit - diabetes prediction pipeline

- Complete data engineering pipeline implementation
- Apache Spark ETL jobs for data processing  
- Airflow DAGs for workflow orchestration
- ML pipeline for diabetes prediction
- Docker Compose for containerized deployment
- Comprehensive testing suite
- Monitoring with Grafana and Prometheus
- GitHub deployment best practices

Co-authored-by: Team Kelompok 8 RA <team@example.com>"
        
        print_status "Initial commit created"
    else
        print_status "Repository already has commits"
    fi
    
    # Set up main branch
    current_branch=$(git branch --show-current)
    if [[ "$current_branch" != "main" ]]; then
        print_info "Current branch is '$current_branch', switching to 'main'..."
        git checkout -b main 2>/dev/null || git checkout main
    fi
    print_status "On main branch"
}

# Function to create GitHub repository
create_github_repository() {
    print_header "🚀 Creating GitHub Repository"
    
    if [[ "$GITHUB_CLI_AVAILABLE" == true && "$GITHUB_CLI_AUTHENTICATED" == true ]]; then
        print_info "Creating repository with GitHub CLI..."
        
        read -p "Enter GitHub organization/username: " github_org
        full_repo_name="$github_org/$REPO_NAME"
        
        # Create repository
        if gh repo create "$full_repo_name" \
            --description "$REPO_DESCRIPTION" \
            --public \
            --clone=false; then
            print_status "Repository created: https://github.com/$full_repo_name"
            
            # Add remote
            git remote add origin "https://github.com/$full_repo_name.git" 2>/dev/null || \
            git remote set-url origin "https://github.com/$full_repo_name.git"
            
            print_status "Remote origin added"
        else
            print_error "Failed to create repository with GitHub CLI"
            manual_setup_instructions
            return
        fi
    else
        print_warning "GitHub CLI not available or not authenticated"
        manual_setup_instructions
        return
    fi
}

# Function to provide manual setup instructions
manual_setup_instructions() {
    print_header "📖 Manual Setup Instructions"
    
    cat << EOF
${YELLOW}Manual GitHub Repository Setup:${NC}

1. Go to https://github.com/new
2. Repository name: ${REPO_NAME}
3. Description: ${REPO_DESCRIPTION}
4. Set as Public repository
5. Don't initialize with README, .gitignore, or license (we have them)
6. Click "Create repository"

7. Add remote and push:
   ${BLUE}git remote add origin https://github.com/YOUR_USERNAME/${REPO_NAME}.git
   git branch -M main
   git push -u origin main${NC}

8. Create develop branch:
   ${BLUE}git checkout -b develop
   git push -u origin develop${NC}

9. Configure repository settings (see GITHUB_DEPLOYMENT_GUIDE.md)

EOF
}

# Function to push to GitHub
push_to_github() {
    print_header "📤 Pushing to GitHub"
    
    # Check if remote exists
    if ! git remote get-url origin &> /dev/null; then
        print_error "No remote 'origin' configured"
        print_info "Please set up GitHub repository first"
        return 1
    fi
    
    print_info "Pushing to GitHub..."
    
    # Push main branch
    if git push -u origin main; then
        print_status "Main branch pushed successfully"
    else
        print_error "Failed to push main branch"
        return 1
    fi
    
    # Create and push develop branch
    print_info "Creating develop branch..."
    git checkout -b develop 2>/dev/null || git checkout develop
    
    if git push -u origin develop; then
        print_status "Develop branch pushed successfully"
    else
        print_warning "Failed to push develop branch (may already exist)"
    fi
    
    # Return to main branch
    git checkout main
}

# Function to provide post-deployment instructions
post_deployment_instructions() {
    print_header "🎯 Post-Deployment Instructions"
    
    cat << EOF
${GREEN}✅ Repository successfully prepared for GitHub!${NC}

${YELLOW}Next steps:${NC}

1. ${BLUE}Repository Settings${NC} (if not done automatically):
   - Go to repository Settings
   - Add topics: ${REPO_TOPICS}
   - Enable Issues, Wiki, Discussions
   - Configure branch protection rules for 'main' branch

2. ${BLUE}Security Settings${NC}:
   - Enable Dependabot alerts
   - Enable Code scanning
   - Enable Secret scanning
   - Review SECURITY.md

3. ${BLUE}Team Setup${NC}:
   - Invite collaborators
   - Update CODEOWNERS with actual GitHub usernames
   - Set up teams (if using GitHub organization)

4. ${BLUE}CI/CD Validation${NC}:
   - Create a test branch and PR
   - Verify GitHub Actions workflows run
   - Check that all tests pass

5. ${BLUE}Documentation Review${NC}:
   - Verify README.md renders correctly
   - Check all links work
   - Update any placeholder information

${BLUE}Useful Commands:${NC}
   View repository: ${YELLOW}gh repo view${NC} (if using GitHub CLI)
   Create issue: ${YELLOW}gh issue create${NC}
   Create PR: ${YELLOW}gh pr create${NC}

${BLUE}Documentation:${NC}
   - See GITHUB_DEPLOYMENT_GUIDE.md for detailed setup
   - See CONTRIBUTING.md for contribution guidelines
   - See SECURITY.md for security policies

${GREEN}🎉 Happy collaborating!${NC}
EOF
}

# Function to create summary report
create_summary_report() {
    print_header "📊 Deployment Summary"
    
    echo "Repository Name: $REPO_NAME"
    echo "Description: $REPO_DESCRIPTION"
    echo "Default Branch: $DEFAULT_BRANCH"
    echo "Topics: $REPO_TOPICS"
    echo ""
    echo "Files Created/Updated:"
    echo "- .github/workflows/ci.yml"
    echo "- .github/dependabot.yml"
    echo "- .github/ISSUE_TEMPLATE/"
    echo "- .github/pull_request_template.md"
    echo "- CONTRIBUTING.md"
    echo "- SECURITY.md"
    echo "- CODEOWNERS"
    echo "- GITHUB_DEPLOYMENT_GUIDE.md"
    echo ""
    echo "Current Git Status:"
    git status --short || true
    echo ""
    echo "Git Remotes:"
    git remote -v || true
}

# Main execution
main() {
    print_header "🚀 GitHub Deployment Setup for Diabetes Prediction Pipeline"
    
    echo "This script will prepare your repository for GitHub deployment with best practices."
    echo ""
    read -p "Continue? (y/N): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Deployment cancelled"
        exit 0
    fi
    
    # Execute deployment steps
    check_prerequisites
    validate_project_structure
    cleanup_repository
    run_security_checks
    prepare_git_repository
    
    # Optionally create GitHub repository
    read -p "Create GitHub repository now? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        create_github_repository
        push_to_github
    else
        print_info "Skipping GitHub repository creation"
        manual_setup_instructions
    fi
    
    create_summary_report
    post_deployment_instructions
    
    print_status "Deployment preparation completed!"
}

# Run main function
main "$@"
