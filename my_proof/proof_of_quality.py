import pandas as pd
import glob
import math
from datetime import datetime

# Constants as provided
class Constants:
    MIN_TIME_SPENT_MS = 2000
    MAX_TIME_SPENT_MS = 7200000
    REQUIRED_FIELDS = {'url', 'timeSpent', 'listOfActions'}
    LONG_DURATION_THRESHOLD_MS = 300000
    MAX_QUALITY_SCORE = 60
    MAX_AUTHENTICITY_SCORE = 40
    HIGH_QUALITY_THRESHOLD = 80
    MODERATE_QUALITY_THRESHOLD = 20
    X0 = 0.5
    K = 5

def combine_csv_files(directory_path):
    """
    Combines multiple CSV files and processes them into the required format.
    """
    # Get all CSV files in the directory
    all_files = glob.glob(f"{directory_path}/*.csv")
    
    # if not all_files:
    #     raise ValueError(f"No CSV files found in {directory_path}")
    
    dfs = []
    for file in all_files:
        df = pd.read_csv(file)
        dfs.append(df)
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Sort by DateTime
    combined_df['DateTime'] = pd.to_datetime(combined_df['DateTime'])
    combined_df = combined_df.sort_values('DateTime', ascending=False)
    
    # Convert to the required format for evaluation
    browsing_data = []
    
    for i in range(len(combined_df)):
        entry = {
            'url': combined_df.iloc[i]['NavigatedToUrl'],
            'timeSpent': 0,  # Will be calculated below
            'listOfActions': []  # Initialize empty list of actions
        }
        
        # Calculate time spent (difference between consecutive timestamps)
        if i < len(combined_df) - 1:
            time_diff = (combined_df.iloc[i]['DateTime'] - 
                        combined_df.iloc[i+1]['DateTime']).total_seconds() * 1000
            entry['timeSpent'] = time_diff
        
        browsing_data.append(entry)
    
    return browsing_data

def is_valid_url(url):
    """Validate URL format"""
    return url.startswith(('http://', 'https://'))

def sigmoid(x, k=Constants.K, x0=Constants.X0):
    """
    Applies the sigmoid function to the normalized score.
    """
    z = k * (x - x0)
    return 1 / (1 + math.exp(-z))

def evaluate_quality(browsing_data):
    """
    Evaluates the quality of the browsing data.
    """
    quality_score = 0
    max_quality_score = Constants.MAX_QUALITY_SCORE
    weights = {
        'time_spent': 40,
        'completeness': 10,
        'action_engagement': 10
    }

    total_entries = len(browsing_data)
    valid_time_entries = 0
    completeness_issues = 0
    action_engagement_score = 0

    for entry in browsing_data:
        # Completeness
        if not Constants.REQUIRED_FIELDS.issubset(entry.keys()):
            completeness_issues += 1
            continue

        # URL validation
        if not is_valid_url(entry['url']):
            completeness_issues += 1
            continue

        # Time Spent Validation
        time_spent = entry['timeSpent']
        if Constants.MIN_TIME_SPENT_MS <= time_spent <= Constants.MAX_TIME_SPENT_MS:
            valid_time_entries += 1

        # Action Engagement
        if entry['listOfActions']:
            action_engagement_score += 1

    # Calculate scores
    if total_entries > 0:
        time_spent_ratio = valid_time_entries / total_entries
        quality_score += time_spent_ratio * weights['time_spent']

        completeness_ratio = (total_entries - completeness_issues) / total_entries
        quality_score += completeness_ratio * weights['completeness']

        action_engagement_ratio = action_engagement_score / total_entries
        quality_score += action_engagement_ratio * weights['action_engagement']

    return min(quality_score, max_quality_score)/100

def evaluate_authenticity(browsing_data):
    """
    Evaluates the authenticity of the browsing data.
    """
    authenticity_score = Constants.MAX_AUTHENTICITY_SCORE
    total_entries = len(browsing_data)
    short_visits = 0
    long_visits_without_actions = 0

    for entry in browsing_data:
        time_spent = entry.get('timeSpent', 0)
        actions = entry.get('listOfActions', [])

        if time_spent < Constants.MIN_TIME_SPENT_MS:
            short_visits += 1

        if time_spent > Constants.LONG_DURATION_THRESHOLD_MS and not actions:
            long_visits_without_actions += 1

    if total_entries > 0:
        short_visit_ratio = short_visits / total_entries
        short_visit_penalty = short_visit_ratio * 20
        authenticity_score -= short_visit_penalty

    long_visit_penalty = long_visits_without_actions * 10
    authenticity_score -= long_visit_penalty

    authenticity_score = max(authenticity_score, 0)
    return authenticity_score/100

def compute_overall_score(quality_score, authenticity_score):
    """
    Combines the quality and authenticity scores.
    """
    overall_score = quality_score + authenticity_score
    return min(overall_score, 1)

def get_quality_label(overall_score):
    """
    Returns a quality label based on the overall score.
    """
    score_percentage = overall_score * 100
    if score_percentage >= Constants.HIGH_QUALITY_THRESHOLD:
        return "High Quality"
    elif score_percentage >= Constants.MODERATE_QUALITY_THRESHOLD:
        return "Moderate Quality"
    else:
        return "Low Quality"

def process_and_evaluate_data(directory_path):
    """
    Main function to process and evaluate browsing data.
    """
    try:
        # Combine and process CSV files
        browsing_data = combine_csv_files(directory_path)
        
        # Calculate scores
        quality_score = evaluate_quality(browsing_data)
        authenticity_score = evaluate_authenticity(browsing_data)
        overall_score = compute_overall_score(quality_score, authenticity_score)
        quality_label = get_quality_label(overall_score)
        
        # Prepare results
        results = {
            "total_entries": len(browsing_data),
            "quality_score": quality_score,
            "authenticity_score": authenticity_score,
            "overall_score": overall_score,
            "quality_label": quality_label
        }
        
        return results
        
    except Exception as e:
        raise Exception(f"Error processing browsing data: {str(e)}")