"""
Metadata Management Module for AutoAI Platform

Handles metadata extraction, enrichment, and storage functionality
"""

from .models import TableMetadata
from .metadata_extractor import GenericMetadataExtractor
from .llm_enricher import MetadataEnricher
from .metadata_store import MetadataStore

__all__ = [
    'GenericMetadataExtractor',
    'TableMetadata',
    'MetadataEnricher',
    'MetadataStore'
]
