import speech_recognition as sr
import tempfile 
import os
from pydub import AudioSegment
from typing import Optional
from shared.logger import Logger
from .config import DataTranscriberConfig

class AudioTranscriber:
    """Audio transcription service using SpeechRecognition library"""
    def __init__(self, config: DataTranscriberConfig):
        self.config = config
        self.recognizer = sr.Recognizer()
        self.logger = Logger.get_logger(
            name="audio_transcriber",
            es_host=config.logger_es_host,
            index=config.logger_index
        )
        
        # Config recognizer settings
        self.recognizer.energy_threshold = 300
        self.recognizer.dynamic_energy_threshold = True
        self.recognizer.pause_threshold = 0.8
        
        self.logger.info("Audio transcriber initiated successfully!")
        
    def transcribe_audio_data(self, audio_data: bytes, filename: str) -> Optional[str]:
        "Transcribe audio data to text"
        try:
            self.logger.info(f"Starting transcription for {filename}...")
            
            # Save audio data to temp file
            with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
                temp_file.write(audio_data)
                temp_audio_path = temp_file.name
                
            try:
                # Load audio with pydub
                audio_segment = AudioSegment.from_wav(temp_audio_path)
                
                # Convert to the format expected by SpeechRecognition
                audio_segment = audio_segment.set_channels(1).set_frame_rate(16000)
                
                # Export to temp wav file
                with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as processed_file:
                    audio_segment.export(processed_file.name, format="wav")
                    processed_audio_path = processed_file.name
                    
                # Transcribe the audio
                transcription = self._transcribe_file(processed_audio_path, filename)
                
                # Clean up temporary files
                os.unlink(processed_audio_path)
                
                return transcription
                
            finally:
                # Clean up original temporary file
                os.unlink(temp_audio_path)
                
        except Exception as e:
            self.logger.error(f"Error transcribing audio for {filename}: {type(e).__name__}: {e}")
            return None
        
    def _transcribe_file(self, audio_path: str, filename: str) -> Optional[str]:
        "Internal method to transcribe audio file"
        try:
            # Load audio file
            with sr.AudioFile(audio_path) as source:
                # Adjust for ambient noise
                self.logger.info(f"Adjusting for ambient noise in {filename}")
                self.recognizer.adjust_for_ambient_noise(source, duration=1)
                
                # Record the audio data
                audio_data = self.recognizer.record(source)
            
            # Perform transcription using Google Speech Recognition
            self.logger.info(f"Performing speech recognition for {filename}")
            transcription = self.recognizer.recognize_google(
                audio_data, 
                language=self.config.speech_recognition_language
            )
            
            # Ensure transcription is a string
            if not isinstance(transcription, str):
                transcription = str(transcription)
            
            self.logger.info(f"Transcription completed successfully for {filename}. Length: {len(transcription)} characters")
            return transcription
            
        except Exception as e:
            self.logger.error(f"Unexpected error during transcription of {filename}: {type(e).__name__}: {e}")
            return None