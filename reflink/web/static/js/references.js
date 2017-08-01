var references_elem = $('#reference-list');

// We send the user back to an endpoint that we control, so that we
//  can reconfigure resolution logic without altering this view.
var getURLforReference = function(reference, document_id, hostname) {
    return 'http://' + hostname + '/references/' + document_id + '/ref/' + encodeURIComponent(reference.identifier) + '/resolve';
}

var renderReferences = function(data, document_id, hostname) {
    data.references.forEach(function(reference) {
        var reference_elem = $('<li class="reference" id="reference-' + reference.identifier + '"></li>');
        if (reference.authors != null) {
            var authors_elem = $('<span class="reference-authors"></span>');
            reference.authors.forEach(function(author) {
                author_elem = $('<span class="reference-author"></span>');
                if (author.givennames) {
                    author_elem.append('<span class="reference-author-givennames">' + author.givennames + '</span>');
                }
                if (author.surname) {
                    author_elem.append('<span class="reference-author-surname">' + author.surname + '</span>');
                }
                authors_elem.append(author_elem);
            });
            reference_elem.append(authors_elem);
        }

        reference_elem.append('<span class="reference-year">' + reference.year + '</span>');
        if (reference.title != null) {
            if (reference.doi == null && reference.identifiers == null) {   // No DOI, so we'll link back to our endpoint.
                reference_elem.append('<span class="reference-title"><a href="' + getURLforReference(reference, document_id, hostname) + '">' + reference.title + '</a></span>');
            } else {
                reference_elem.append('<span class="reference-title">' + reference.title + '</span>');
            }
        }

        if (reference.source != null && reference.source.indexOf('=') == -1) {
            // Temporary patch for bad source parsing...
            if (reference.source.indexOf('DOI') > -1) {
                reference.source = reference.source.split('DOI')[0].trim();
            }
            reference_elem.append('<span class="reference-source">' + reference.source + '</span>');
        }
        if (reference.volume != null) {
            reference_elem.append('<span class="reference-volume">' + reference.volume + '</span>');
        }
        if (reference.issue != null) {
            reference_elem.append('<span class="reference-issue">' + reference.issue + '</span>');
        }
        if (reference.pages != null) {
            reference_elem.append('<span class="reference-pages">' + reference.pages + '</span>');
        }
        if (reference.doi != null) {
            reference_elem.append('<span class="reference-doi"><a href="' + getURLforReference(reference, document_id, hostname) + '">' + reference.doi + '</a></span>');
        }
        if (reference.identifiers != null) {
            reference.identifiers.forEach(function(identifier) {
                var identifier_elem = $('<span class="reference-identifier"></span>');
                identifier_elem.append('<span class="reference-identifier-type">' + identifier.identifier_type + '</span>');
                if (identifier.identifier_type == 'arxiv') {
                    identifier_elem.append('<span class="reference-identifier-value"><a href="' + getURLforReference(reference, document_id, hostname) + '">' + identifier.identifier + '</a></span>');
                }
                reference_elem.append(identifier_elem);
            });
        }
        references_elem.append(reference_elem);
    });
}

var renderError = function(err) {
    if (err.status == 404) {
        references_elem.append('No reference data available for this paper.');
    } else {
        references_elem.append('Whoops! Something went wrong. Please contact an administrator if the problem persists.');
    }
}

var loadReferences = function(document_id, hostname) {
    $.get('http://' + hostname + '/references/' + document_id,
          function(data) {
              console.log(data);
              renderReferences(data, document_id, hostname);
          }).fail(renderError);
}
