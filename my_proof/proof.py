import json
import logging
import os
from typing import Dict, Any
import requests
from jwt import encode as jwt_encode
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

from my_proof.proof_of_authenticity import calculate_authenticity_score
from my_proof.proof_of_ownership import calculate_ownership_score, generate_jwt_token
from my_proof.proof_of_quality import calculate_quality_score
from my_proof.proof_of_uniqueness import uniqueness_helper
from my_proof.models.proof_response import ProofResponse

# Ensure logging is configured
logging.basicConfig(level=logging.INFO)


CONTRIBUTION_THRESHOLD = 4
EXTRA_POINTS = 5

class Proof:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.proof_response = ProofResponse(dlp_id=config['dlp_id'])

    def generate(self) -> ProofResponse:
        """Generate proofs for all input files."""
        logging.info("Starting proof generation")

        proof_response_object = {
            'dlp_id': self.config.get('dlp_id', '24'),
            'valid': True,
        }

        for input_filename in os.listdir(self.config['input_dir']):
            input_file = os.path.join(self.config['input_dir'], input_filename)
            if os.path.splitext(input_file)[1].lower() == '.json':
                with open(input_file, 'r', encoding='utf-8') as f:
                    input_data = json.load(f)

                logging.info(f"Processing file: {input_filename}")
               
                # proof_response_object['ownership'] = 1.0
                wallet_w_types = self.extract_wallet_address_and_types(input_data) 
                proof_response_object['ownership'] = self.calculate_ownership_score(wallet_w_types)
                input_hash_details = uniqueness_helper(input_data)
                unique_entry_details = input_hash_details.get("unique_entries")
                proof_response_object['uniqueness'] = input_hash_details.get("uniqueness_score")
                proof_response_object['quality'] = self.calculate_quality_score(input_data, unique_entry_details)
                proof_response_object['authenticity'] = self.calculate_authenticity_score(input_data)

                if proof_response_object['authenticity'] < 1.0:
                    proof_response_object['valid'] = False

                # Calculate the final score
                proof_response_object['score'] = self.calculate_final_score(proof_response_object)

                # proof_response_object['attributes'] = {
                #     # 'normalizedContributionScore': contribution_score_result['normalized_dynamic_score'],
                #     # 'totalContributionScore': contribution_score_result['total_dynamic_score'],
                # }

        logging.info(f"Proof response: {proof_response_object}")
        return proof_response_object

    def generate_jwt_token(self, wallet_address):
        secret_key = self.config.get('jwt_secret_key', 'default_secret')
        expiration_time = self.config.get('jwt_expiration_time', 16000)  # Set to 10 minutes (600 seconds)
        
        # Set the expiration time to 10 minutes from now
        exp = datetime.now(timezone.utc) + timedelta(seconds=expiration_time)
        
        payload = {
            'exp': exp,
            'walletAddress': wallet_address  # Add wallet address to the payload
        }
        
        # Encode the JWT
        token = jwt_encode(payload, secret_key, algorithm='HS256')
        return token

    def extract_wallet_address_and_types(self, input_data):
        wallet_address = input_data.get('walletAddress')
        types = [contribution.get('type') for contribution in input_data.get('contribution', [])]
        return  {'walletAddress': wallet_address, 'types': types}

    def calculate_authenticity_score(self, input_data: Dict[str, Any]) -> float:
        """Calculate authenticity score."""
        contributions = input_data.get('contribution', [])
        valid_domains = ["wss://witness.reclaimprotocol.org/ws", "reclaimprotocol.org"]
        return calculate_authenticity_score(contributions, valid_domains)

    def calculate_ownership_score(self, input_data: Dict[str, Any]) -> float:
        """Calculate ownership score."""
        wallet_address = input_data.get('walletAddress')
        types = input_data.get('types', [])
        data = {
            'walletAddress': wallet_address,
            'types': types
        }
        
        jwt_token = generate_jwt_token(wallet_address, self.config.get('jwt_secret_key'), self.config.get('jwt_expiration_time', 16000))
        return calculate_ownership_score(jwt_token, data, self.config.get('validator_base_api_url'))
    
    def calculate_quality_score(self, input_data, unique_entries):
        return calculate_quality_score(input_data, self.config, unique_entries)
    
    def calculate_final_score(self, proof_response_object: Dict[str, Any]) -> float:
        attributes = ['authenticity', 'uniqueness', 'quality', 'ownership']
        weights = {
            'authenticity': 0.003,  # Low weight for authenticity
            'ownership': 0.005,  # Slightly higher than authenticity
            'uniqueness': 0.342,  # Moderate weight for uniqueness
            'quality': 0.650  # High weight for quality
        }

        weighted_sum = 0.0
        for attr in attributes:
            weighted_sum += proof_response_object.get(attr, 0) * weights[attr]

        return weighted_sum