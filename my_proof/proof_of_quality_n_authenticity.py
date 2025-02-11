import logging
import pandas as pd
import math
from datetime import datetime
import json
from typing import List, Dict, Any, Optional, Union
from collections import Counter
from dateutil import parser


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

def process_unique_csv_data(unique_csv_data):
    """
    Processes the unique CSV data DataFrame into the required format.
    """
    # Ensure 'DateTime' is in datetime format
    unique_csv_data['DateTime'] = pd.to_datetime(unique_csv_data['DateTime'])
    
    # Sort by DateTime in descending order
    unique_csv_data = unique_csv_data.sort_values('DateTime', ascending=False)
    
    # Initialize the list to hold processed browsing data
    browsing_data = []
    
    for i in range(len(unique_csv_data)):
        entry = {
            'url': unique_csv_data.iloc[i]['NavigatedToUrl'],
            'timeSpent': 0,  # Will be calculated below
            'listOfActions': []  # Initialize empty list of actions
        }
        
        # Calculate time spent (difference between consecutive timestamps)
        if i < len(unique_csv_data) - 1:
            time_diff = (unique_csv_data.iloc[i]['DateTime'] - 
                         unique_csv_data.iloc[i + 1]['DateTime']).total_seconds() * 1000
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

    return min(quality_score, max_quality_score) / 100

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
    return authenticity_score / 100

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

def process_and_evaluate_data(unique_csv_data):
    """
    Main function to process and evaluate browsing data from unique_csv_data.
    """
    try:
        # Process the unique CSV data
        browsing_data = process_unique_csv_data(unique_csv_data)
        
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


class AndroidLocationHistoryValidator:
    def __init__(self, max_speed_m_s: float = 44.44, allowed_hierarchy_levels: List[int] = [0, 1, 2]):
        self.max_speed_m_s = max_speed_m_s
        self.allowed_hierarchy_levels = allowed_hierarchy_levels
        self.max_walk_speed = 1.4
        self.max_run_speed = 3.5
        
    @staticmethod
    def parse_time(time_str: str) -> Optional[datetime]:
        if not time_str:
            return None
        try:
            # Android format uses ISO 8601 with timezone
            return parser.parse(time_str)
        except Exception:
            return None

    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371_000  # Earth radius in meters
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = phi2 - phi1
        dlambda = math.radians(lon2 - lon1)

        a = (math.sin(dphi / 2)**2 
             + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    @staticmethod
    def calc_speed(distance_meters: float, t1: datetime, t2: datetime) -> float:
        if not t1 or not t2:
            return 0.0
        dt = (t2 - t1).total_seconds()
        return distance_meters / dt if dt > 0 else 0.0

    def check_time_order(self, data: List[Dict[str, Any]]) -> float:
        if not data:
            return 1.0
            
        issues = 0
        total_checks = len(data) * 2 - 1  # Two checks per entry plus transitions
        
        for i, entry in enumerate(data):
            start = self.parse_time(entry.get("startTime"))
            end = self.parse_time(entry.get("endTime"))
            if start and end and end < start:
                issues += 1
            if i < len(data) - 1:
                next_start = self.parse_time(data[i + 1].get("startTime"))
                if end and next_start and next_start < end:
                    issues += 1
        
        return 1.0 - (issues / total_checks)

    def check_suspicious_speed(self, data: List[Dict[str, Any]]) -> float:
        if not data:
            return 1.0
            
        valid_entries = 0
        total_checked = 0
        
        for entry in data:
            if "activities" in entry and entry["activities"]:
                total_checked += 1
                distance = entry.get("distance", 0)
                start_time = self.parse_time(entry.get("startTime"))
                end_time = self.parse_time(entry.get("endTime"))

                try:
                    dist_m = float(distance) if distance else 0.0
                except ValueError:
                    dist_m = 0.0

                speed = self.calc_speed(dist_m, start_time, end_time)
                if speed <= self.max_speed_m_s:
                    valid_entries += 1
        
        return valid_entries / total_checked if total_checked > 0 else 1.0

    def check_inconsistent_probabilities(self, data: List[Dict[str, Any]]) -> float:
        if not data:
            return 1.0
            
        valid_probs = 0
        total_probs = 0
        
        for entry in data:
            if "activities" in entry:
                for activity in entry["activities"]:
                    if "probability" in activity:
                        total_probs += 1
                        try:
                            prob = float(activity["probability"])
                            if 0.0 <= prob <= 1.0:
                                valid_probs += 1
                        except ValueError:
                            pass
        
        return valid_probs / total_probs if total_probs > 0 else 1.0

    def check_hierarchy_levels(self, data: List[Dict[str, Any]]) -> float:
        if not data:
            return 1.0
        valid_levels = 0
        total_checked = 0
        
        for entry in data:
            if "placeVisit" in entry:
                place = entry["placeVisit"].get("location", {})
                if "locationConfidence" in place:
                    total_checked += 1
                    try:
                        confidence = float(place["locationConfidence"])
                        if 0.0 <= confidence <= 1.0:
                            valid_levels += 1
                    except ValueError:
                        pass
        
        return valid_levels / total_checked if total_checked > 0 else 1.0

    def check_waypoints(self, data: List[Dict[str, Any]]) -> float:
        if not data:
            return 1.0
            
        valid_points = 0
        total_points = 0
        
        for entry in data:
            if "activitySegment" in entry:
                waypoints = entry["activitySegment"].get("waypointPath", {}).get("waypoints", [])
                for waypoint in waypoints:
                    total_points += 2  # Two checks per point: lat and lng
                    
                    if "latE7" in waypoint and "lngE7" in waypoint:
                        try:
                            lat = float(waypoint["latE7"]) / 1e7
                            lng = float(waypoint["lngE7"]) / 1e7
                            if -90 <= lat <= 90 and -180 <= lng <= 180:
                                valid_points += 2
                        except ValueError:
                            pass
        
        return valid_points / total_points if total_points > 0 else 1.0

    def check_for_regular_intervals(self, data: List[Dict[str, Any]]) -> float:
        if not data:
            return 1.0
            
        intervals = []
        for i in range(len(data) - 1):
            end_cur = self.parse_time(data[i].get("endTime"))
            start_next = self.parse_time(data[i + 1].get("startTime"))
            if end_cur and start_next:
                intervals.append((start_next - end_cur).total_seconds())

        if not intervals:
            return 1.0

        c = Counter(intervals)
        uniqueness_ratio = len(c) / len(intervals)
        return uniqueness_ratio

    def check_local_travel_vs_mode(self, data: List[Dict[str, Any]]) -> float:
        if not data:
            return 1.0
            
        valid_modes = 0
        total_checked = 0
        
        for entry in data:
            if "activitySegment" in entry:
                activity_type = entry["activitySegment"].get("activityType", "").lower()
                if not (activity_type and ("walking" in activity_type or "running" in activity_type)):
                    continue
                    
                total_checked += 1
                start_time = self.parse_time(entry["activitySegment"].get("startTime"))
                end_time = self.parse_time(entry["activitySegment"].get("endTime"))
                
                try:
                    dist_m = float(entry["activitySegment"].get("distance", 0))
                except ValueError:
                    dist_m = 0.0

                speed = self.calc_speed(dist_m, start_time, end_time)
                
                if ("walking" in activity_type and speed <= self.max_walk_speed) or \
                   ("running" in activity_type and speed <= self.max_run_speed):
                    valid_modes += 1
        
        return valid_modes / total_checked if total_checked > 0 else 1.0

    def check_time_span(self, data: List[Dict[str, Any]]) -> float:
        if not data:
            return 0.0
        
        earliest_time = None
        latest_time = None
        
        for entry in data:
            start = self.parse_time(entry.get("startTime"))
            end = self.parse_time(entry.get("endTime"))
            
            if start:
                if earliest_time is None or start < earliest_time:
                    earliest_time = start
            if end:
                if latest_time is None or end > latest_time:
                    latest_time = end
        
        if earliest_time and latest_time:
            return ((latest_time - earliest_time).total_seconds())/86400.0
        return 0.0

    def validate(self, data: List[Dict[str, Any]]) -> float:
        # Android data is already a list of segments
        segments = data
        print(f"\nStarting validation with {len(segments)} segments")
        
        checks = [
            ("Time Order", self.check_time_order(segments)),
            ("Suspicious Speed", self.check_suspicious_speed(segments)),
            ("Probabilities", self.check_inconsistent_probabilities(segments)),
            ("Hierarchy Levels", self.check_hierarchy_levels(segments)),
            ("Waypoints", self.check_waypoints(segments)),
            ("Regular Intervals", self.check_for_regular_intervals(segments)),
            ("Local Travel", self.check_local_travel_vs_mode(segments))
        ]
        
        print("\nIndividual check results:")
        for name, value in checks:
            print(f"{name}: {value:.3f}")
            
        valid = sum(value for _, value in checks)
        print(f"\nSum of all checks: {valid:.3f}")
        print(f"Minimum threshold: {7*0.1}")
        
        if valid < (7*0.1):
            print("Failed validation - returning -1")
            return -1
        
        time_span = self.check_time_span(segments)
        print(f"\nTime span in days: {time_span:.2f}")
        print(f"Time span score (divided by 60): {time_span/60.0:.3f}")
        
        final_score = min(time_span/60.0, 1.0)
        print(f"Final clamped score: {final_score:.3f}")
        
        return final_score


def process_files_for_quality_n_authenticity_scores(unique_csv_data, unique_json_data, unique_yaml_data):

    if unique_csv_data is None or unique_csv_data.empty:
        total_csv_entries = 0
    else:
        total_csv_entries = unique_csv_data.drop_duplicates().shape[0]

    if not unique_json_data or not isinstance(unique_json_data, list) or not unique_json_data[0]:
        semantic_segments_data = []
        total_json_entries = 0
    else:
        logging.info(f"unique json data: {unique_json_data[0].get('semanticSegments')}")
        semantic_segments_data = unique_json_data[0].get("semanticSegments", [])
        total_json_entries = len(semantic_segments_data)
    
    if not unique_yaml_data or not isinstance(unique_yaml_data, list) or not unique_yaml_data[0]:
        total_yaml_entries = 0
    else:
        total_yaml_entries = len(unique_yaml_data[0])

    # Validate JSON data using AndroidLocationHistoryValidator
    location_history_quality_score = 0.0
    location_history_authenticity_score = 0.0
    if total_json_entries > 0:
        validator = AndroidLocationHistoryValidator()
        location_history_quality_score = validator.validate(semantic_segments_data)
        location_history_authenticity_score = 1.0  # Default authenticity score for location data

    # Evaluate unique CSV data using process_and_evaluate_data
    browser_history_quality_score = 0.0
    browser_history_authenticity_score = 0.0
    if total_csv_entries > 0:
        browser_history_score_details = process_and_evaluate_data(unique_csv_data)
        browser_history_quality_score = browser_history_score_details.get("quality_score", 0)
        browser_history_authenticity_score = browser_history_score_details.get("authenticity_score", 0)

    # Evaluate quality of YAML data
    if total_yaml_entries > 10:
        yaml_quality_score = 1.0 
    elif 4 < total_yaml_entries <= 9:
        yaml_quality_score = 1.0 * 0.5
    elif 1 < total_yaml_entries <= 4:
        yaml_quality_score = 1.0 * 0.10
    else:
        yaml_quality_score = 0.0

    if yaml_quality_score > 0 : 
        yaml_authenticity_score = 1.0
    else:
        yaml_authenticity_score = 0.0

    # Determine final quality and authenticity scores
    final_quality_score = 0.0
    final_authenticity_score = 0.0

    if total_csv_entries > 0 and total_json_entries > 0:
        final_quality_score = (
            (browser_history_quality_score * total_csv_entries) + (location_history_quality_score * total_json_entries) + (yaml_quality_score * total_yaml_entries)
        ) / (total_csv_entries + total_json_entries + total_yaml_entries)

        final_authenticity_score = (
            (browser_history_authenticity_score * total_csv_entries) + (location_history_authenticity_score * total_json_entries + yaml_authenticity_score * total_yaml_entries)
        ) / (total_csv_entries + total_json_entries + total_yaml_entries)

    elif total_csv_entries > 0:
        final_quality_score = browser_history_quality_score
        final_authenticity_score = browser_history_authenticity_score

    elif total_json_entries > 0:
        final_quality_score = location_history_quality_score
        final_authenticity_score = location_history_authenticity_score
    
    elif total_yaml_entries > 0:
        final_quality_score = yaml_quality_score
        final_authenticity_score = yaml_authenticity_score

    logging.info(f"Final Quality Score: {final_quality_score}, Final Authenticity Score: {final_authenticity_score}, Total CSV Entries: {total_csv_entries}, Total JSON Entries: {total_json_entries}, Browser History Quality Score: {browser_history_quality_score}, Browser History Authenticity Score: {browser_history_authenticity_score}, Location History Quality Score: {location_history_quality_score}, Location History Authenticity Score: {location_history_authenticity_score}, Total YAML Entries: {total_yaml_entries}, YAML Quality Score: {yaml_quality_score}, YAML Authenticity Score: {yaml_authenticity_score}")

    # Return final scores
    return {
        "quality_score": final_quality_score,
        "authenticity_score": final_authenticity_score
    }