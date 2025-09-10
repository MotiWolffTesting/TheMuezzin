from typing import Dict
from datetime import datetime
from shared.logger import Logger
from shared.config import SharedConfig


class BDSThreatAnalyzer:
    """BDS threat detection analyzer"""
    
    def __init__(self):
        self.config = SharedConfig.load_env()
        self.logger = Logger.get_logger(
            name="bds_threat_analyzer",
            es_host=self.config.logger_es_host,
            index=self.config.logger_index
        )
        
        # High threat keywords - gets 2x weight 
        self.high_threat_keywords = [
            "genocide", "war crimes", "apartheid", "massacre", "nakba", 
            "displacement", "humanitarian crisis", "blockade", "occupation", 
            "refugees", "icc", "bds"
        ]
        
        # Medium threat keywords - gets 1x weight  
        self.medium_threat_keywords = [
            "freedom flotilla", "resistance", "liberation", "free palestine", 
            "gaza", "ceasefire", "protest", "unrwa"
        ]
        
        # Word pairs that are problematic together
        self.threat_word_pairs = [
            ("israel", "apartheid"), ("israel", "genocide"), ("gaza", "massacre"),
            ("free", "palestine"), ("boycott", "israel")
        ]
        
        # Set thresholds from config
        self.bds_threshold_low = self.config.bds_threshold_low
        self.bds_threshold_medium = self.config.bds_threshold_medium  
        self.bds_threshold_high = self.config.bds_threshold_high 
        
        self.logger.info("BDS threat analyzer initiated successfully")
        
    def analyze_text(self, text: str, filename: str = "") -> Dict:
        "Analyze text for BDS indicators"
        if not text:
            return self._create_empty_analysis(filename)
        
        text_lower = text.lower()
        
        # Count keywords
        high_count = sum(text_lower.count(keyword) for keyword in self.high_threat_keywords)
        medium_count = sum(text_lower.count(keyword) for keyword in self.medium_threat_keywords)
        
        # Count word pairs
        pair_count = sum(1 for word1, word2 in self.threat_word_pairs 
                        if word1 in text_lower and word2 in text_lower)
        
        # Calculate BDS percentage based on word coverage methodology
        total_words = len(text_lower.split())
        if total_words == 0:
            bds_percentage = 0.0
        else:
            # High threat words get 2x weight, medium get 1x, pairs get 3x
            weighted_score = (high_count * 2) + (medium_count * 1) + (pair_count * 3)
            # Calculate percentage based on coverage of threatening words
            bds_percentage = min((weighted_score / total_words) * 100, 100.0)
        
         # Determine threat level and is_bds using configurable thresholds
        is_bds = bds_percentage >= self.config.bds_threshold_medium
        if bds_percentage >= self.config.bds_threshold_high:
            threat_level = 'high'
        elif bds_percentage >= self.config.bds_threshold_medium:
            threat_level = 'medium'
        elif bds_percentage >= self.config.bds_threshold_low:
            threat_level = 'low'
        else:
            threat_level = 'none'
        
        return {
            'filename': filename,
            'timestamp': datetime.now().isoformat(),
            'bds_percentage': round(bds_percentage, 1),
            'is_bds': is_bds,
            'threat_level': threat_level,
            'bds_threat_level': threat_level,
            'high_threat_count': high_count,
            'medium_threat_count': medium_count,
            'pair_count': pair_count
        }
    
    def _create_empty_analysis(self, filename: str) -> Dict:
        "Create empty analysis"
        return {
            'filename': filename,
            'timestamp': datetime.now().isoformat(),
            'bds_percentage': 0.0,
            'is_bds': False,
            'threat_level': 'none',
            'bds_threat_level': 'none',
            'high_threat_count': 0,
            'medium_threat_count': 0,
            'pair_count': 0
        }
    
    
    