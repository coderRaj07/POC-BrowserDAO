import logging
import os
from eth_account import Account
from eth_account.messages import encode_defunct

def recover_account(author: str, signature: str, random_string: str) -> bool:
    """
    Recover Ethereum account from signature and verify ownership.

    :param author: Ethereum address of the supposed signer
    :param signature: Signature to verify
    :param random_string: Random string used for signing
    :return: True if the recovered account matches the author, False otherwise
    """
    if not author or not signature or not random_string:
        logging.error("Missing required fields: author, signature, or random_string")
        return False

    try:
        message_encoded = encode_defunct(text=random_string)
        recovered_address = Account.recover_message(message_encoded, signature=signature)
        
        if recovered_address.lower() == author.lower():
            logging.info(f"Ownership verified successfully for address: {recovered_address}")
            return True
        else:
            logging.warning(f"Recovered address {recovered_address} does not match author {author}")
            return False
    except Exception as e:
        logging.error(f"Error during recovery: {e}")
        return False

def read_params_from_file(file_path: str):
    """
    Read parameters from a text file.

    :param file_path: Path to the text file
    :return: Tuple containing author, signature, and random_string
    """
    params = {}
    with open(file_path, "r") as file:
        for line in file:
            key, value = line.strip().split(": ", 1)
            params[key] = value
    return params["author"], params["signature"], params["random_string"]

def verify_ownership(input_dir: str) -> float:
    """Verify ownership by checking the signature in a .txt file."""
    logging.info(f"Verifying ownership in directory: {input_dir}")
    txt_files = [f for f in os.listdir(input_dir) if f.endswith('.txt')]
    logging.info(f"Found {len(txt_files)} .txt file(s) for ownership verification.")
    if not txt_files:
        logging.warning("No .txt file found for ownership verification.")
        return 0.0

    txt_file_path = os.path.join(input_dir, txt_files[0])
    author, signature, random_string = read_params_from_file(txt_file_path)
    logging.info(f" author: {author} signature: {signature}, env_sign: {os.environ.get("SIGNATURE")} random_string {random_string}")
    is_valid = recover_account(author, signature, random_string) and os.environ.get("SIGNATURE") == signature
    return 1.0 if is_valid else 0.0
