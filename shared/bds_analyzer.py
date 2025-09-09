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
        
        # Set thresholds
        self.bds_threshold = 20.0  
        
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
        
        # Calculate BDS percentage
        score = (high_count * 2) + (medium_count * 1) + (pair_count * 3)
        bds_percentage = min(score * 5, 100.0)
        
        # Determine threat level and is_bds
        is_bds = bds_percentage >= self.bds_threshold
        if bds_percentage >= 50:
            threat_level = 'high'
        elif bds_percentage >= 20:
            threat_level = 'medium'
        elif bds_percentage >= 5:
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
    
    